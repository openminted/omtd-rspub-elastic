from elasticsearch import Elasticsearch

from omtdrspub.elastic.model.change_doc import ChangeDoc
from omtdrspub.elastic.model.resource_doc import ResourceDoc


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

    def es_instance(self) -> Elasticsearch:
        return Elasticsearch([{"host": self.host, "port": self.port}])

    def create_index(self, index, mapping):
        return self._instance.indices.create(index=index, body=mapping, ignore=400)

    def delete_index(self, index):
        return self._instance.indices.delete(index=index, ignore=404)

    def index_resource(self, index, resource_doc_type, resource_doc: ResourceDoc):
        return self._instance.index(index=index, doc_type=resource_doc_type, body=resource_doc.to_dict())

    def index_change(self, index, change_doc_type, change_doc: ChangeDoc):
        return self._instance.index(index=index, doc_type=change_doc_type, body=change_doc.to_dict())

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
        self.refresh_index(index=index)

    def refresh_index(self, index):
        return self._instance.indices.refresh(index=index)

    def scan_and_scroll(self, index, doc_type, query, max_items_in_list, max_result_window):
        result_size = max_items_in_list
        c_iter = 0
        n_iter = 1
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
