import os
from os.path import basename
from urllib.parse import urljoin

import logging
from resync import Resource
from resync import ResourceList
from resync.list_base_with_index import ListBaseWithIndex
from rspub.core.executors import Executor, SitemapData, ExecutorEvent
from rspub.core.rs_enum import Capability
from rspub.util import defaults

from omtdrspub.elastic.elastic_query_manager import ElasticQueryManager
from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.resource_doc import ResourceDoc

MAX_RESULT_WINDOW = 10000

LOG = logging.getLogger(__name__)


class ElasticResourceListExecutor(Executor):
    def __init__(self, rs_parameters: ElasticRsParameters):
        super(ElasticResourceListExecutor, self).__init__(rs_parameters)
        self.query_manager = ElasticQueryManager(self.para.elastic_host, self.para.elastic_port)

    def execute(self, filenames=None):
        # filenames is not necessary, we use it only to match the method signature
        self.date_start_processing = defaults.w3c_now()
        self.observers_inform(self, ExecutorEvent.execution_start, date_start_processing=self.date_start_processing)
        if not os.path.exists(self.para.abs_metadata_dir()):
            os.makedirs(self.para.abs_metadata_dir())

        self.prepare_metadata_dir()
        sitemap_data_iter = self.generate_rs_documents()
        self.post_process_documents(sitemap_data_iter)
        self.date_end_processing = defaults.w3c_now()
        self.create_index(sitemap_data_iter)

        capabilitylist_data = self.create_capabilitylist()
        self.update_resource_sync(capabilitylist_data)

        self.observers_inform(self, ExecutorEvent.execution_end, date_end_processing=self.date_end_processing,
                              new_sitemaps=sitemap_data_iter)
        return sitemap_data_iter

    def prepare_metadata_dir(self):
        if self.para.is_saving_sitemaps:
            self.clear_metadata_dir()

    def generate_rs_documents(self, filenames: iter = None) -> [SitemapData]:
        self.query_manager.refresh_index(self.para.elastic_index)
        # filenames is not necessary, we use it only to match the method signature
        sitemap_data_iter = []
        generator = self.resourcelist_generator()
        for sitemap_data, sitemap in generator():
            sitemap_data_iter.append(sitemap_data)

        return sitemap_data_iter

    def create_index(self, sitemap_data_iter: iter):
        if len(sitemap_data_iter) > 1:
            resourcelist_index = ResourceList()
            resourcelist_index.sitemapindex = True
            resourcelist_index.md_at = self.date_start_processing
            resourcelist_index.md_completed = self.date_end_processing
            index_path = self.para.abs_metadata_path("resourcelist-index.xml")
            rel_index_path = os.path.relpath(index_path, self.para.resource_dir)
            index_url = urljoin(self.para.url_prefix, defaults.sanitize_url_path(rel_index_path))
            resourcelist_index.link_set(rel="up", href=self.para.capabilitylist_url())
            for sitemap_data in sitemap_data_iter:
                resourcelist_index.add(Resource(uri=sitemap_data.uri, md_at=sitemap_data.doc_start,
                                                md_completed=sitemap_data.doc_end))
                if sitemap_data.document_saved:
                    LOG.info("Updating document: " + basename(sitemap_data.path))
                    self.update_rel_index(index_url, sitemap_data.path)

            self.finish_sitemap(-1, resourcelist_index)

    def save_sitemap(self, sitemap, path):
        sitemap.pretty_xml = self.para.is_saving_pretty_xml
        # writing the string sitemap.as_xml() to disk results in encoding=ASCII on some systems.
        # due to https://docs.python.org/3.4/library/xml.etree.elementtree.html#write
        #sitemap.write(path)
        if sitemap.sitemapindex:
            self.write_index(sitemap, path)
        else:
            sitemap.write(path)

    @staticmethod
    def write_index(sitemap: ListBaseWithIndex, path):
        """Return XML serialization of this list taken to be sitemapindex entries

        """
        sitemap.default_capability()
        s = sitemap.new_sitemap()
        return s.resources_as_xml(sitemap, sitemapindex=True, fh=path)

    def resourcelist_generator(self) -> iter:

        def generator() -> [SitemapData, ResourceList]:
            resourcelist = None
            ordinal = self.find_ordinal(Capability.resourcelist.name)
            resource_count = 0
            doc_start = None
            resource_generator = self.resource_generator()
            for resource_count, resource in resource_generator():
                # stuff resource into resourcelist
                if resourcelist is None:
                    resourcelist = ResourceList()
                    doc_start = defaults.w3c_now()
                    resourcelist.md_at = doc_start
                resourcelist.add(resource)

                # under conditions: yield the current resourcelist
                if resource_count % self.para.max_items_in_list == 0:
                    ordinal += 1
                    doc_end = defaults.w3c_now()
                    resourcelist.md_completed = doc_end
                    LOG.info("Generating resourcelist #:" + str(ordinal) + "...")
                    sitemap_data = self.finish_sitemap(ordinal, resourcelist, doc_start=doc_start, doc_end=doc_end)
                    LOG.info("Resource list # " + str(ordinal) + " successfully generated")
                    yield sitemap_data, resourcelist
                    resourcelist = None

            # under conditions: yield the current and last resourcelist
            if resourcelist:
                ordinal += 1
                doc_end = defaults.w3c_now()
                resourcelist.md_completed = doc_end
                # if ordinal == 0:
                # if we have a single doc, set ordinal to -1 so that the finish_sitemap will not append the
                # ordinal to the filename
                # ordinal = -1
                # print("Generating resourcelist")
                # else:
                LOG.info("Generating resourcelist #:" + str(ordinal) + "...")
                sitemap_data = self.finish_sitemap(ordinal, resourcelist, doc_start=doc_start, doc_end=doc_end)
                LOG.info("Resource list # " + str(ordinal) + " successfully generated")
                yield sitemap_data, resourcelist

        return generator

    def resource_generator(self) -> iter:

        def generator(count=0) -> [int, Resource]:
            elastic_page_generator = self.elastic_page_generator()
            erased_changes = False
            for e_page in elastic_page_generator():
                if not erased_changes:
                    # this will happen at the first scroll
                    self.erase_changes()
                    LOG.info("Changes erased")
                    erased_changes = True
                for e_hit in e_page:
                    e_source = e_hit['_source']
                    e_doc = ResourceDoc.as_resource_doc(e_source)
                    count += 1
                    uri = e_doc.location.uri_from_path(para_url_prefix=self.para.url_prefix,
                                                       para_res_root_dir=self.para.res_root_dir)
                    ln = []
                    if e_doc.ln:
                        for link in e_doc.ln:
                            link_uri = link.href.uri_from_path(para_url_prefix=self.para.url_prefix,
                                                               para_res_root_dir=self.para.res_root_dir)
                            ln.append({'href': link_uri, 'rel': link.rel, 'mime': link.mime})

                    resource = Resource(uri=uri, length=e_doc.length,
                                        lastmod=e_doc.lastmod,
                                        md5=e_doc.md5,
                                        mime_type=e_doc.mime,
                                        ln=ln)
                    yield count, resource
                    self.observers_inform(self, ExecutorEvent.created_resource, resource=resource,
                                          count=count)

        return generator

    def elastic_page_generator(self) -> iter:

        def generator() -> iter:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {"resource_set": self.para.resource_set}
                            }
                        ]
                    }
                }
            }

            return self.query_manager.scan_and_scroll(index=self.para.elastic_index,
                                                      doc_type=self.para.elastic_resource_doc_type,
                                                      query=query,
                                                      max_items_in_list=self.para.max_items_in_list,
                                                      max_result_window=MAX_RESULT_WINDOW)

        return generator

    def erase_changes(self):
        self.query_manager.delete_all_index_set_type_docs(index=self.para.elastic_index,
                                                          doc_type=self.para.elastic_change_doc_type,
                                                          resource_set=self.para.resource_set)
