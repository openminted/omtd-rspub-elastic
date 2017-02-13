#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from abc import ABCMeta
from glob import glob
from urllib.parse import urljoin

from resync import ChangeList
from resync import Resource
from resync import ResourceList
from resync.sitemap import Sitemap
from rspub.core.executors import Executor, SitemapData, ExecutorEvent
from rspub.core.rs_enum import Capability
from rspub.util import defaults

from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.elastic_utils import ElasticChangeDoc, es_page_generator, es_get_instance

MAX_RESULT_WINDOW = 10000


class ElasticChangeListExecutor(Executor, metaclass=ABCMeta):

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

    def generate_rs_documents(self, filenames: iter=None) -> [SitemapData]:
        pass

    def __init__(self, rs_parameters: ElasticRsParameters=None):
        Executor.__init__(self, rs_parameters)

        # next parameters will all be set in the method update_previous_state
        self.previous_changes = None
        self.date_resourcelist_completed = None
        self.date_changelist_from = None
        self.resourcelist_files = []
        self.changelist_files = []
        ##

    def create_index(self, sitemap_data_iter: iter) -> SitemapData:
        changelist_index_path = self.para.abs_metadata_path("changelist-index.xml")
        changelist_index_uri = self.para.uri_from_path(changelist_index_path)
        if os.path.exists(changelist_index_path):
            os.remove(changelist_index_path)

        changelist_files = sorted(glob(self.para.abs_metadata_path("changelist_*.xml")))
        if len(changelist_files) > 1:
            changelist_index = ChangeList()
            changelist_index.sitemapindex = True
            changelist_index.md_from = self.date_resourcelist_completed
            for cl_file in changelist_files:
                changelist = self.read_sitemap(cl_file, ChangeList())
                uri = self.para.uri_from_path(cl_file)
                changelist_index.resources.append(Resource(uri=uri, md_from=changelist.md_from,
                                                           md_until=changelist.md_until))

                if self.para.is_saving_sitemaps:
                    index_link = changelist.link("index")
                    if index_link is None:
                        changelist.link_set(rel="index", href=changelist_index_uri)
                        self.save_sitemap(changelist, cl_file)

            self.finish_sitemap(-1, changelist_index)

    def update_previous_state(self):
        if self.previous_changes is None:
            self.previous_changes = {}

            # search for resourcelists
            self.resourcelist_files = sorted(glob(self.para.abs_metadata_path("resourcelist_*.xml")))
            for rl_file_name in self.resourcelist_files:
                resourcelist = ResourceList()
                with open(rl_file_name, "r", encoding="utf-8") as rl_file:
                    sm = Sitemap()
                    sm.parse_xml(rl_file, resources=resourcelist)

                self.date_resourcelist_completed = resourcelist.md_completed
                if self.date_resourcelist_completed is None:
                    self.date_resourcelist_completed = resourcelist.md_at

                # self.previous_resources.update({resource.uri: resource for resource in resourcelist.resources})

            # search for changelists
            self.changelist_files = sorted(glob(self.para.abs_metadata_path("changelist_*.xml")))
            for cl_file_name in self.changelist_files:
                changelist = ChangeList()
                with open(cl_file_name, "r", encoding="utf-8") as cl_file:
                    sm = Sitemap()
                    sm.parse_xml(cl_file, resources=changelist)

                for r_change in changelist.resources:
                    self.previous_changes.update({r_change.uri: r_change})

    def changelist_generator(self) -> iter:

        def generator(changelist=None) -> [SitemapData, ChangeList]:
            new_changes = {}
            resource_generator = self.resource_generator()
            self.update_previous_state()
            prev_changes = self.previous_changes
            es_changes = [resource for count, resource in resource_generator()]

            for r_change in es_changes:
                new_changes.update({r_change.uri: r_change})

            created = [r for r in new_changes.values() if r.change == "created"]
            updated = [r for r in new_changes.values() if r.change == "updated"]
            deleted = [r for r in new_changes.values() if r.change == "deleted" and
                       (True if (r.uri in prev_changes and prev_changes[r.uri].change != "deleted") else False)]

            num_created = len(created)
            num_updated = len(updated)
            num_deleted = len(deleted)
            tot_changes = num_created + num_updated + num_deleted
            self.observers_inform(self, ExecutorEvent.found_changes, created=num_created, updated=num_updated,
                                  deleted=num_deleted)
            all_changes = {"created": created, "updated": updated, "deleted": deleted}

            ordinal = self.find_ordinal(Capability.changelist.name)

            resource_count = 0
            if changelist:
                ordinal -= 1
                resource_count = len(changelist)
                if resource_count >= self.para.max_items_in_list:
                    changelist = None
                    ordinal += 1
                    resource_count = 0

            for kv in all_changes.items():
                for r_change in kv[1]:
                    if changelist is None:
                        changelist = ChangeList()
                        changelist.md_from = self.date_changelist_from

                    r_change.change = kv[0] # type of change: created, updated or deleted
                    r_change.md_datetime = self.date_start_processing
                    changelist.add(r_change)
                    resource_count += 1

                    # under conditions: yield the current changelist
                    if resource_count % self.para.max_items_in_list == 0:
                        ordinal += 1
                        sitemap_data = self.finish_sitemap(ordinal, changelist)
                        yield sitemap_data, changelist
                        changelist = None

            # under conditions: yield the current and last changelist
            if changelist and tot_changes > 0:
                ordinal += 1
                sitemap_data = self.finish_sitemap(ordinal, changelist)
                yield sitemap_data, changelist

        return generator

    def resource_generator(self) -> iter:

        def generator(count=0) -> [int, Resource]:
            elastic_page_generator = self.elastic_page_generator()
            for e_page in elastic_page_generator():
                for e_hit in e_page:
                    e_source = e_hit['_source']
                    e_doc = ElasticChangeDoc(e_hit['_id'], e_source['rel_path'], e_source['lastmod'],
                                             e_source['change'], e_source['res_set'], e_source['res_type'])
                    count += 1
                    # path = os.path.relpath(file, self.para.resource_dir)
                    uri = urljoin(self.para.url_prefix, defaults.sanitize_url_path(e_doc.rel_path)) 
                    resource = Resource(uri=uri,
                                        lastmod=e_doc.lastmod,
                                        change=e_doc.change)
                    yield count, resource
                    self.observers_inform(self, ExecutorEvent.created_resource, resource=resource,
                                          count=count)

        return generator

    def elastic_page_generator(self) -> iter:
        changes_since = self.para.changes_since if hasattr(self.para, 'changes_since') \
            else self.date_resourcelist_completed

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {"res_set": self.para.res_set}
                        },
                        {
                            "term": {"res_type": self.para.res_type}
                        },
                        {
                            "range": {"lastmod": {"gte": changes_since}}
                        }
                    ]
                }
            },
            "sort": [
                {
                    "_timestamp": {
                        "order": "asc"
                    }
                }
            ]
        }

        return es_page_generator(es_get_instance(self.para.elastic_host, self.para.elastic_port),
                                 self.para.elastic_index, self.para.elastic_change_type, query, self.para.max_items_in_list, MAX_RESULT_WINDOW)


class ElasticNewChangeListExecutor(ElasticChangeListExecutor):
    """
    :samp:`Implements the new changelist strategy`

    A :class:`NewChangeListExecutor` creates new changelists every time the executor runs (and is_saving_sitemaps).
    If there are previous changelists that are not closed (md:until is not set) this executor will close
    those previous changelists by setting their md:until value to now (start_of_processing)
    """
    def generate_rs_documents(self, filenames: iter=None):
        self.update_previous_state()
        if len(self.changelist_files) == 0:
            self.date_changelist_from = self.date_resourcelist_completed
        else:
            self.date_changelist_from = self.date_start_processing

        sitemap_data_iter = []
        generator = self.changelist_generator()
        for sitemap_data, changelist in generator():
            sitemap_data_iter.append(sitemap_data)

        return sitemap_data_iter

    def post_process_documents(self, sitemap_data_iter: iter):
        # change md:until value of older changelists - if we created new changelists.
        # self.changelist_files was globed before new documents were generated (self.update_previous_state).
        if self.para.is_saving_sitemaps:
            for filename in self.changelist_files:
                changelist = self.read_sitemap(filename, ChangeList())
                if changelist.md_until is None:
                    changelist.md_until = self.date_start_processing
                    self.save_sitemap(changelist, filename)


class ElasticIncrementalChangeListExecutor(ElasticChangeListExecutor):
    """
    :samp:`Implements the incremental changelist strategy`

    An :class:`IncrementalChangeListExecutor` adds changes to an already existing changelist every time
    the executor runs
    (and is_saving_sitemaps).
    """
    def generate_rs_documents(self, filenames: iter=None):
        self.update_previous_state()
        self.date_changelist_from = self.date_resourcelist_completed
        changelist = None
        if len(self.changelist_files) > 0:
            changelist = self.read_sitemap(self.changelist_files[-1], ChangeList())

        sitemap_data_iter = []
        generator = self.changelist_generator()

        for sitemap_data, changelist in generator(changelist=changelist):
            sitemap_data_iter.append(sitemap_data)

        return sitemap_data_iter


