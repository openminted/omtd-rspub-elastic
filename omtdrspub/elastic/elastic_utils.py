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
