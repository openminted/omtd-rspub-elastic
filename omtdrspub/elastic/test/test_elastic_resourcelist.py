import os
import unittest
from urllib.parse import urljoin
from resync import Resource
from resync import ResourceList
from rspub.util import defaults

from omtdrspub.elastic.exe_elastic_resourcelist import ElasticResourceDoc

res_dir = "/test/path/"
prefix = "http://example.com"


def compose_uri(path):
    path = os.path.relpath(path, res_dir)
    uri = urljoin(prefix, defaults.sanitize_url_path(path))
    return uri


class TestElasticResourceList(unittest.TestCase):

    def test_url(self):
        file_path = "/test/path/file1.txt"
        self.assertEqual(compose_uri(file_path), "http://example.com/file1.txt")

    def test_elastic_resourcelist(self):
        resourcelist = ResourceList()
        doc1 = ElasticResourceDoc("file1", "/test/path/file1.txt", 5, "md5", "text/plain", "2017-02-03T12:25:00Z", "elsevier",
                                  "metadata", [{"href": "file1.pdf", "rel": "describes", "mime": "application/pdf"}])
        doc2 = ElasticResourceDoc("file2", "/test/path/file2.txt", 6, "md5", "text/plain", "2017-02-03T12:27:00Z", "elsevier",
                                  "metadata", [{"href": "file2.pdf", "rel": "describes", "mime": "application/pdf"}])
        docs = [doc1, doc2]
        for doc in docs:
            path = os.path.relpath(doc.file_path, res_dir)
            uri = compose_uri(path)

            for link in doc.ln:
                link_uri = compose_uri(link['href'])
                link['href'] = link_uri

            resource = Resource(uri=uri, length=doc.size,
                                lastmod=doc.time,
                                md5=doc.md5,
                                mime_type=doc.mime,
                                ln=doc.ln)
            resourcelist.add(resource)
        self.assertEqual(len(resourcelist.resources), 2, 'ResourceList with 2 resources')

if __name__ == '__main__':
    unittest.main()
