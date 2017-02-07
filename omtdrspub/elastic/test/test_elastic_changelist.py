import os
import unittest
from urllib.parse import urljoin

from resync import ChangeList
from resync import Resource
from rspub.util import defaults

from omtdrspub.elastic.exe_elastic_changelist import ElasticChangeDoc

res_dir = "/test/path/"
prefix = "http://example.com"


def compose_uri(path):
    path = os.path.relpath(path, res_dir)
    uri = urljoin(prefix, defaults.sanitize_url_path(path))
    return uri


def changelist_pipeline(old_changes, new_es_changes):
    # create a test changelist with some changes
    changelist = add_changes_to_changelist(old_changes)

    # get new changes from ES
    new_changes = get_changes_from_es_docs(new_es_changes)
    # merge changes applied on the same resource
    m_old_changes = merge_resource_changes(changelist.resources)
    m_new_changes = merge_resource_changes(new_changes)
    all_changes = filter_changes(m_old_changes, m_new_changes)
    return all_changes


def filter_changes(previous_changes, new_changes):
    created = [r for r in new_changes.values() if r.change == "created"]
    updated = [r for r in new_changes.values() if r.change == "updated"]
    deleted = [r for r in new_changes.values() if r.change == "deleted" and
               (True if (r.uri in previous_changes and previous_changes[r.uri].change != "deleted") else False)]
    all_changes = {"created": created, "updated": updated, "deleted": deleted}
    return all_changes


def add_changes_to_changelist(changes, changelist=None):
    if changelist is None:
        changelist = ChangeList()

    for change in changes:
        changelist.add(change)
    return changelist


def merge_resource_changes(changes):
    final_changes = {}
    for r_change in changes:
        final_changes.update({r_change.uri: r_change})
    return final_changes


def get_changes_from_es_docs(docs):
    new_resources = []

    # new_resources
    for doc in docs:
        change = Resource(uri=compose_uri(doc.file_path), change=doc.change, lastmod=doc.lastmod)
        new_resources.append(change)
    return new_resources


class TestElasticResourceList(unittest.TestCase):

    # def merge_changes(self, previous_changes, new_changes):
    #     created = [r for r in new_changes.values() if r.change == "created" and
    #                (True if (
    #                    r.uri not in previous_changes or (
    #                        r.uri in previous_changes and previous_changes[r.uri].change != "created")) else False)]
    #     updated = [r for r in new_changes.values() if r.change == "updated" and
    #                (True if (
    #                    r.uri not in previous_changes or (
    #                        r.uri in previous_changes and r.lastmod != previous_changes[r.uri].lastmod)) else False)]
    #     deleted = [r for r in new_changes.values() if r.change == "deleted" and
    #                (True if (
    #                    r.uri not in previous_changes or (
    #                        r.uri in previous_changes and previous_changes[r.uri].change != "deleted")) else False)]

    #     return created, updated, deleted

    def test_inc_changes_with_old_resources(self):

        #changelist
        old_change1 = Resource(uri="http://example.com/file1.txt", change="created", lastmod="2017-02-01T00:00:00Z",
                               md_datetime="2017-02-01T05:00:00Z")
        old_change2 = Resource(uri="http://example.com/file2.txt", change="created", lastmod="2017-02-01T00:00:00Z",
                               md_datetime="2017-02-01T05:00:00Z")

        #docs
        doc1 = ElasticChangeDoc("change1", "/test/path/file1.txt", "2017-02-03T14:27:00Z", "deleted", "elsevier",
                                "metadata")
        doc2 = ElasticChangeDoc("change2", "/test/path/file2.txt", "2017-02-03T14:27:00Z", "updated", "elsevier",
                                "metadata")

        all_changes = changelist_pipeline([old_change1, old_change2], [doc1, doc2])

        self.assertTrue(len(all_changes["created"]) + len(all_changes["updated"]) + len(all_changes["deleted"]) == 2)

    def test_uneffective_changes(self):
        # docs
        doc1 = ElasticChangeDoc("change1", "/test/path/file1.txt", "2017-02-03T14:27:00Z", "created", "elsevier",
                                "metadata")
        doc2 = ElasticChangeDoc("change2", "/test/path/file1.txt", "2017-02-03T14:28:00Z", "updated", "elsevier",
                                "metadata")
        doc3 = ElasticChangeDoc("change3", "/test/path/file1.txt", "2017-02-03T14:29:00Z", "deleted", "elsevier",
                                "metadata")

        all_changes = changelist_pipeline([], [doc1, doc2, doc3])

        self.assertTrue(len(all_changes["created"]) + len(all_changes["updated"]) + len(all_changes["deleted"]) == 0)

    def test_changes_without_old_resources(self):

        #docs
        doc1 = ElasticChangeDoc("change1", "/test/path/file1.txt", "2017-02-03T14:27:00Z", "deleted", "elsevier",
                                "metadata")
        doc2 = ElasticChangeDoc("change2", "/test/path/file2.txt", "2017-02-03T14:27:00Z", "updated", "elsevier",
                                "metadata")

        new_changes = get_changes_from_es_docs([doc1, doc2])
        final_changes = merge_resource_changes(new_changes)

        #new_changes
        changelist = add_changes_to_changelist(final_changes.values())

        self.assertTrue(len(changelist.resources) == 2)

    def test_merge_resource_new_changes(self):

        # docs
        doc1 = ElasticChangeDoc("change1", "/test/path/file1.txt", "2017-02-03T14:27:00Z", "created", "elsevier",
                                "metadata")
        doc2 = ElasticChangeDoc("change2", "/test/path/file1.txt", "2017-02-03T14:28:00Z", "updated", "elsevier",
                                "metadata")
        doc3 = ElasticChangeDoc("change3", "/test/path/file1.txt", "2017-02-03T14:29:00Z", "deleted", "elsevier",
                                "metadata")
        doc4 = ElasticChangeDoc("change4", "/test/path/file1.txt", "2017-02-03T14:30:00Z", "created", "elsevier",
                                "metadata")
        doc5 = ElasticChangeDoc("change5", "/test/path/file1.txt", "2017-02-03T14:31:00Z", "updated", "elsevier",
                                "metadata")

        all_changes = changelist_pipeline([], [doc1, doc2, doc3, doc4, doc5])

        self.assertTrue("http://example.com/file1.txt" in [r.uri for r in all_changes["updated"]] and
                        len(all_changes["created"]) == 0 and len(all_changes["deleted"]) == 0 and
                        len(all_changes["updated"]) == 1)

    def test_merge_resource_old_and_new_changes(self):
        # old changes
        old_change1 = Resource(uri="http://example.com/file1.txt", change="created", lastmod="2017-02-01T00:00:00Z",
                               md_datetime="2017-02-01T05:00:00Z")
        old_change2 = Resource(uri="http://example.com/file2.txt", change="created", lastmod="2017-02-01T00:00:00Z",
                               md_datetime="2017-02-01T05:00:00Z")

        # new es changes
        doc1 = ElasticChangeDoc("change1", "/test/path/file1.txt", "2017-02-03T14:27:00Z", "updated", "elsevier",
                                "metadata")
        doc2 = ElasticChangeDoc("change2", "/test/path/file1.txt", "2017-02-03T14:28:00Z", "updated", "elsevier",
                                "metadata")
        doc3 = ElasticChangeDoc("change3", "/test/path/file1.txt", "2017-02-03T14:29:00Z", "deleted", "elsevier",
                                "metadata")
        doc4 = ElasticChangeDoc("change4", "/test/path/file1.txt", "2017-02-03T14:30:00Z", "created", "elsevier",
                                "metadata")
        doc5 = ElasticChangeDoc("change5", "/test/path/file1.txt", "2017-02-03T14:31:00Z", "updated", "elsevier",
                                "metadata")

        all_changes = changelist_pipeline([old_change1, old_change2], [doc1, doc2, doc3, doc4, doc5])

        self.assertTrue("http://example.com/file1.txt" in [r.uri for r in all_changes["updated"]] and
                        len(all_changes["created"]) == 0 and len(all_changes["deleted"]) == 0 and
                        len(all_changes["updated"]) == 1)


