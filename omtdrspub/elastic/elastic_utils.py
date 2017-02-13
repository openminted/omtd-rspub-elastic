import os

import yaml
from elasticsearch import Elasticsearch

from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters


def es_get_instance(host, port):
    return Elasticsearch([{"host": host, "port": port}])


def es_create_index(es, index, mapping):
    return es.indices.create(index=index, body=mapping, ignore=400)


def es_delete_index(es, index):
    return es.indices.delete(index=index, ignore=404)


def es_put_resource(es, index, resource_type, res_id, rel_path, res_set, res_type, length, md5, mime, lastmod, ln):
    doc = {
        "rel_path": rel_path,
        "length": length,
        "md5": md5,
        "mime": mime,
        "lastmod": lastmod,
        "res_set": res_set,
        "res_type": res_type,
        "ln": ln
    }
    return es.index(index=index, doc_type=resource_type, body=doc,
                    id=res_id)


def es_put_change(es, index, resource_type, rel_path, res_set, res_type, change, lastmod):
    doc = {
        "rel_path": rel_path,
        "lastmod": lastmod,
        "change": change,
        "res_set": res_set,
        "res_type": res_type
    }

    return es.index(index=index, doc_type=resource_type, body=doc)


def es_refresh_index(es, index):
    return es.indices.refresh(index=index)


def parse_yaml_params(config_file):

    f = open(config_file, 'r+')
    config = yaml.load(f)['executor']

    if not os.path.exists(config['description_dir']):
        os.makedirs(config['description_dir'])

    rs_params = ElasticRsParameters(**config)
    return rs_params


def es_page_generator(es, es_index, es_type, query, max_items_in_list, max_result_window):
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

    page = es.search(index=es_index, doc_type=es_type, scroll='2m',
                     size=result_size,
                     body=query)
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
        page = es.scroll(scroll_id=sid, scroll='2m')
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


class ElasticResourceDoc(object):
    def __init__(self, elastic_id, rel_path, length, md5, mime, time, res_set, res_type, ln):
        self._elastic_id = elastic_id
        self._rel_path = rel_path
        self._length = length
        self._md5 = md5
        self._mime = mime
        self._time = time
        self._res_set = res_set
        self._res_type = res_type
        self._ln = ln

    @property
    def elastic_id(self):
        return self.elastic_id

    @property
    def rel_path(self):
        return self._rel_path

    @property
    def length(self):
        return self._length

    @property
    def md5(self):
        return self._md5

    @property
    def mime(self):
        return self._mime

    @property
    def time(self):
        return self._time

    @property
    def res_set(self):
        return self._res_set

    @property
    def res_type(self):
        return self._res_type

    @property
    def ln(self):
        return self._ln


class ElasticChangeDoc(object):
    def __init__(self, elastic_id, rel_path, lastmod, change, res_set, res_type):
        self._elastic_id = elastic_id
        self._rel_path = rel_path
        self._lastmod = lastmod
        self._change = change
        self._res_set = res_set
        self._res_type = res_type

    @property
    def elastic_id(self):
        return self.elastic_id

    @property
    def rel_path(self):
        return self._rel_path

    @property
    def lastmod(self):
        return self._lastmod

    @property
    def change(self):
        return self._change

    @property
    def res_set(self):
        return self._res_set

    @property
    def res_type(self):
        return self._res_type
