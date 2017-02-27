import os
from urllib.parse import urljoin

import yaml
from elasticsearch import Elasticsearch
from resync.sitemap import RS_NS, SitemapParseError, SITEMAP_NS, SitemapIndexError
from rspub.util import defaults

from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters

from xml.etree.ElementTree import parse
from resync.resource_container import ResourceContainer


def parse_xml_without_urls(sm, fh=None, etree=None, resources=None, capability=None,
                          sitemapindex=None):
    """
    This is a modification of the Sitemap.parse_xml method of the resync library
    We need to parse rs:md and rs:ln items, avoiding to parse <url> tags
    """
    if resources is None:
        resources = ResourceContainer()
    if fh is not None:
        etree = parse(fh)
    elif etree is None:
        raise ValueError("Neither fh or etree set")
    # check root element: urlset (for sitemap), sitemapindex or bad
    root_tag = etree.getroot().tag
    resource_tag = None  # will be <url> or <sitemap> depending on type
    sm.parsed_index = None
    if root_tag == '{' + SITEMAP_NS + "}urlset":
        sm.parsed_index = False
        if sitemapindex is not None and sitemapindex:
            raise SitemapIndexError(
                "Got sitemap when expecting sitemapindex", etree)
        resource_tag = '{' + SITEMAP_NS + "}url"
    elif root_tag == '{' + SITEMAP_NS + "}sitemapindex":
        sm.parsed_index = True
        if sitemapindex is not None and not sitemapindex:
            raise SitemapIndexError(
                "Got sitemapindex when expecting sitemap", etree)
        resource_tag = '{' + SITEMAP_NS + "}sitemap"
    else:
        raise SitemapParseError(
            "XML is not sitemap or sitemapindex (root element is <%s>)"
            "" % root_tag)

    # have what we expect, read it
    in_preamble = True
    sm.resources_created = 0
    seen_top_level_md = False
    for e in etree.getroot().getchildren():
        # look for <rs:md> and <rs:ln>, first <url> ends
        # then look for resources in <url> blocks
        if e.tag == resource_tag:
            pass
            # in_preamble = False  # any later rs:md or rs:ln is error
            # r = self.resource_from_etree(e, self.resource_class)
            # try:
            #     resources.add(r)
            # except SitemapDupeError:
            #     self.logger.warning(
            #         "dupe of: %s (lastmod=%s)" % (r.uri, r.lastmod))
            # self.resources_created += 1
        elif e.tag == "{" + RS_NS + "}md":
            if in_preamble:
                if seen_top_level_md:
                    raise SitemapParseError(
                        "Multiple <rs:md> at top level of sitemap")
                else:
                    resources.md = sm.md_from_etree(e, 'preamble')
                    seen_top_level_md = True
            else:
                raise SitemapParseError(
                    "Found <rs:md> after first <url> in sitemap")
        elif e.tag == "{" + RS_NS + "}ln":
            if in_preamble:
                resources.ln.append(sm.ln_from_etree(e, 'preamble'))
            else:
                raise SitemapParseError(
                    "Found <rs:ln> after first <url> in sitemap")
        else:
            # element we don't recognize, ignore
            pass
    # check that we read to right capability document
    if capability is not None:
        if 'capability' not in resources.md:
            if capability == 'resourcelist':
                sm.logger.warning(
                    'No capability specified in sitemap, assuming '
                    'resourcelist')
                resources.md['capability'] = 'resourcelist'
            else:
                raise SitemapParseError(
                    "Expected to read a %s document, but no capability "
                    "specified in sitemap" % capability)
        if resources.md['capability'] != capability:
            raise SitemapParseError("Expected to read a %s document, got %s" % (capability, resources.md['capability']))
    # return the resource container object
    return resources

def es_get_instance(host, port):
    return Elasticsearch([{"host": host, "port": port}])


def es_create_index(es, index, mapping):
    return es.indices.create(index=index, body=mapping, ignore=400)


def es_delete_index(es, index):
    return es.indices.delete(index=index, ignore=404)


def es_put_resource(es, index, resource_type, res_id, location, res_set, length, md5, mime, lastmod, ln):
    doc = {
        "location": location,
        "length": length,
        "md5": md5,
        "mime": mime,
        "lastmod": lastmod,
        "res_set": res_set,
        "ln": ln
    }
    return es.index(index=index, doc_type=resource_type, body=doc,
                    id=res_id)


def es_put_change(es, index, resource_type, location, res_set, change, lastmod):
    doc = {
        "location": location,
        "lastmod": lastmod,
        "change": change,
        "res_set": res_set,
    }

    return es.index(index=index, doc_type=resource_type, body=doc)


def es_delete_all_documents(es, index, doc_type):
    query = {
        "query":
            {
                "match_all": {}
            }
        }
    es.delete_by_query(index=index, doc_type=doc_type, body=query)


def es_refresh_index(es, index):
    return es.indices.refresh(index=index)


def es_uri_from_location(loc, para_url_prefix, para_res_root_dir):
    uri = None
    if loc['type'] == 'url':
        uri = loc['value']
    elif loc['type'] == 'rel_path':
        uri = urljoin(para_url_prefix, defaults.sanitize_url_path(loc['value']))
    elif loc['type'] == 'abs_path':
        path = os.path.relpath(loc['value'], para_res_root_dir)
        uri = para_url_prefix + defaults.sanitize_url_path(path)
    return uri


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
    def __init__(self, elastic_id, location, length, md5, mime, time, res_set, ln):
        self._elastic_id = elastic_id
        self._location = location
        self._length = length
        self._md5 = md5
        self._mime = mime
        self._time = time
        self._res_set = res_set
        self._ln = ln

    @property
    def elastic_id(self):
        return self.elastic_id

    @property
    def location(self):
        return self._location

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
    def ln(self):
        return self._ln


class ElasticChangeDoc(object):
    def __init__(self, elastic_id, location, lastmod, change, res_set):
        self._elastic_id = elastic_id
        self._location = location
        self._lastmod = lastmod
        self._change = change
        self._res_set = res_set

    @property
    def elastic_id(self):
        return self.elastic_id

    @property
    def location(self):
        return self._location

    @property
    def lastmod(self):
        return self._lastmod

    @property
    def change(self):
        return self._change

    @property
    def res_set(self):
        return self._res_set

