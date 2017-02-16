import os
from urllib.parse import urljoin

from resync import Resource
from resync import ResourceList
from rspub.core.executors import Executor, SitemapData, ExecutorEvent
from rspub.core.rs_enum import Capability
from rspub.util import defaults

from omtdrspub.elastic.elastic_utils import ElasticResourceDoc, es_page_generator, es_get_instance, es_uri_from_location

MAX_RESULT_WINDOW = 10000


class ElasticResourceListExecutor(Executor):
    def __init__(self, rs_parameters):
        super(ElasticResourceListExecutor, self).__init__(rs_parameters)

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
                    self.update_rel_index(index_url, sitemap_data.path)

            self.finish_sitemap(-1, resourcelist_index)

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
                    print("Generating resourcelist #:" + str(ordinal))
                    sitemap_data = self.finish_sitemap(ordinal, resourcelist, doc_start=doc_start, doc_end=doc_end)
                    print("Finish")
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
                print("Generating resourcelist #:" + str(ordinal))
                sitemap_data = self.finish_sitemap(ordinal, resourcelist, doc_start=doc_start, doc_end=doc_end)
                print("Finish")
                yield sitemap_data, resourcelist

        return generator

    def resource_generator(self) -> iter:

        def generator(count=0) -> [int, Resource]:
            elastic_page_generator = self.elastic_page_generator()
            for e_page in elastic_page_generator():
                for e_hit in e_page:
                    e_source = e_hit['_source']
                    e_doc = ElasticResourceDoc(e_hit['_id'], e_source['location'], e_source['length'], e_source['md5'],
                                               e_source['mime'], e_source['lastmod'], e_source['res_set'],
                                               e_source['res_type'], e_source['ln'])
                    count += 1
                    # path = os.path.relpath(file, self.para.resource_dir)
                    # uri = urljoin(self.para.url_prefix, defaults.sanitize_url_path(path))
                    uri = es_uri_from_location(loc=e_doc.location, para_url_prefix=self.para.url_prefix,
                                               para_res_root_dir=self.para.res_root_dir)
                    if e_doc.ln:
                        for link in e_doc.ln:
                            # link_path = os.path.relpath(link['href'], self.para.resource_dir)
                            # link_uri = urljoin(self.para.url_prefix, defaults.sanitize_url_path(link_path))
                            link_uri = es_uri_from_location(loc=link['href'], para_url_prefix=self.para.url_prefix,
                                                            para_res_root_dir=self.para.res_root_dir)
                            #link_uri = urljoin(self.para.url_prefix, defaults.sanitize_url_path(link['href']))
                            link['href'] = link_uri

                    resource = Resource(uri=uri, length=e_doc.length,
                                        lastmod=e_doc.time,
                                        md5=e_doc.md5,
                                        mime_type=e_doc.mime,
                                        ln=e_doc.ln)
                    yield count, resource
                    self.observers_inform(self, ExecutorEvent.created_resource, resource=resource,
                                          count=count)

        return generator

    def elastic_page_generator(self) -> iter:

        def generator() -> iter:
            query = {"query":
                        {"bool":
                            {"must": [
                                {"term":
                                     {"res_set": self.para.res_set}
                                 },

                                {"term":
                                     {"res_type": self.para.res_type}
                                 }]
                            }
                        }
                    }

            return es_page_generator(es_get_instance(self.para.elastic_host, self.para.elastic_port),
                                    self.para.elastic_index, self.para.elastic_resource_type, query,
                                    self.para.max_items_in_list, MAX_RESULT_WINDOW)

        return generator
