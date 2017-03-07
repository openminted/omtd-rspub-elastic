from elasticsearch import Elasticsearch
from rspub.util import defaults

from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.change_doc import ChangeDoc
from omtdrspub.elastic.model.location import Location
from omtdrspub.elastic.model.resource_doc import ResourceDoc


def location_query(resource_set, location: Location):
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"resource_set": resource_set}
                    },
                    {
                        "nested": {
                            "path": "location",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term":
                                             {"location.type": location.loc_type}
                                         },
                                        {"term":
                                            {
                                                "location.value": location.value}
                                        }
                                    ]
                                }
                            }

                        }
                    }
                ]
            }
        }
    }


def resync_id_query(resource_set, resync_id):
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"resource_set": resource_set}
                    },
                    {
                        "term": {"resync_id": resync_id}
                    }
                ]
            }
        }
    }


class ResourceAlreadyExistsException(TypeError):
    pass


class DuplicateResourceException(TypeError):
    pass


class ElasticQueryManager:
    def __init__(self, host: str, port: str):
        self._host = host
        self._port = port
        self._instance = self.es_instance()

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def resource_exists(self, index, doc_type, resource_set, location):
        return False if self.get_resource_by_location(index=index, doc_type=doc_type,
                                                      resource_set=resource_set, location=location) is None else True

    def get_resource_by_location(self, index, doc_type, resource_set, location: Location):
        query = location_query(resource_set=resource_set, location=location)
        result = self._instance.search(index=index, doc_type=doc_type, body=query)
        hits = [ResourceDoc.as_resource_doc(hit['_source']) for hit in result['hits']['hits']]
        if len(hits) == 0:
            return None
        elif len(hits) > 1:
            raise DuplicateResourceException('Error: more than one resource with location: %s' % location.to_dict())
        elif len(hits) == 1:
            return hits[0]

    def get_resource_by_resync_id(self, index, doc_type, resource_set, resync_id):
        query = resync_id_query(resource_set=resource_set, resync_id=resync_id)
        result = self._instance.search(index=index, doc_type=doc_type, body=query)
        hits = [ResourceDoc.as_resource_doc(hit['_source']) for hit in result['hits']['hits']]

        if len(hits) == 0:
            return None
        elif len(hits) > 1:
            raise DuplicateResourceException('Error: more than one resource with resync_id: %s' % resync_id)
        elif len(hits) == 1:
            return hits[0]

    def get_resource_by_elastic_id(self, index, doc_type, elastic_id):
        return self._instance.get(index=index, doc_type=doc_type, id=elastic_id).get('_source')

    def es_instance(self) -> Elasticsearch:
        return Elasticsearch([{"host": self.host, "port": self.port}], timeout=30, max_retries=10,
                             retry_on_timeout=True)

    def create_index(self, index, mapping):
        return self._instance.indices.create(index=index, body=mapping, ignore=400)

    def delete_index(self, index):
        return self._instance.indices.delete(index=index, ignore=404)

    def delete_document(self, index, doc_type, elastic_id):
        return self._instance.delete(index=index, doc_type=doc_type, id=elastic_id)

    def index_resource(self, index, resource_doc_type, resource_doc: ResourceDoc, elastic_id=None, op_type='index'):
        return self._instance.index(index=index, doc_type=resource_doc_type, body=resource_doc.to_dict(),
                                    id=elastic_id, op_type=op_type)

    def delete_resource_by_resync_id(self, index, resource_doc_type, resource_set, resync_id):
        query = resync_id_query(resource_set=resource_set, resync_id=resync_id)
        return self._instance.delete_by_query(index=index, doc_type=resource_doc_type, body=query)

    def delete_resource_by_location(self, index, resource_doc_type, resource_set, location: Location):
        query = location_query(resource_set=resource_set, location=location)
        return self._instance.delete_by_query(index=index, doc_type=resource_doc_type, body=query)

    def index_change(self, index, change_doc_type, change_doc: ChangeDoc):
        return self._instance.index(index=index, doc_type=change_doc_type, body=change_doc.to_dict())

    def index_bulk(self, index, doc_type, body):
        return self._instance.bulk(index=index, doc_type=doc_type, body=body, refresh=True)

    def delete_all_index_set_type_docs(self, index, doc_type, resource_set):
        query = {"query":
            {"bool":
                {"must": [
                    {"term":
                         {"resource_set": resource_set}
                     }
                ]
                }
            }
        }
        self._instance.delete_by_query(index=index, doc_type=doc_type, body=query)

    def refresh_index(self, index):
        return self._instance.indices.refresh(index=index)

    def scan_and_scroll(self, index, doc_type, query, max_items_in_list, max_result_window):
        result_size = max_items_in_list
        n_iter = 1
        c_iter = 0
        # index.max_result_window in Elasticsearch controls the max number of results returned from a query.
        # we can either increase it to 50k in order to match the sitemaps pagination requirements or not
        # in the latter case, we have to bulk the number of items that we want to put into each resourcelist chunk
        if max_items_in_list > max_result_window:
            n = max_items_in_list / max_result_window
            n_iter = int(n)
            result_size = max_result_window

        page = self._instance.search(index=index, doc_type=doc_type, scroll='2m', size=result_size, body=query)
        sid = page['_scroll_id']
        # total_size = page['hits']['total']
        scroll_size = len(page['hits']['hits'])
        bulk = page['hits']['hits']
        c_iter += 1
        # if c_iter and n_iter control the number of iteration we need to perform in order to yield a bulk of
        #  (at most) self.para.max_items_in_list
        if c_iter >= n_iter or scroll_size < result_size:
            c_iter = 0
            yield bulk
            bulk = []

        while scroll_size > 0:
            page = self._instance.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])
            bulk.extend(page['hits']['hits'])
            c_iter += 1
            if c_iter >= n_iter or scroll_size < result_size:
                c_iter = 0
                yield bulk
                bulk = []

    # high level resource handling
    def create_resource(self, params: ElasticRsParameters, resync_id, location, length, md5, mime, lastmod,
                        ln=None, elastic_id=None, record_change=True):

        index = params.elastic_index

        if self.resource_exists(index=index, doc_type=params.elastic_resource_doc_type,
                                resource_set=params.resource_set, location=location):
            raise ResourceAlreadyExistsException('Error: resource %s already exists in set %s. (%s)'
                                                 % (resync_id, params.resource_set, location.to_dict()))

        resource_doc = ResourceDoc(resync_id=resync_id, resource_set=params.resource_set, location=location,
                                   length=length, md5=md5, mime=mime, lastmod=lastmod, ln=ln)
        self.index_resource(index=index, resource_doc_type=params.elastic_resource_doc_type,
                            resource_doc=resource_doc, elastic_id=elastic_id, op_type='create')

        if record_change:
            change_doc = ChangeDoc(resource_set=params.resource_set,
                                   location=location, lastmod=lastmod, change='created', datetime=defaults.w3c_now())
            self.index_change(index=index, change_doc_type=params.elastic_change_doc_type, change_doc=change_doc)
