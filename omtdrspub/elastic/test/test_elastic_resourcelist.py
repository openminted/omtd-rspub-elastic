import os
import shutil
import unittest
from urllib.parse import urljoin

from rspub.util import defaults

from omtdrspub.elastic.elastic_generator import ElasticGenerator
from omtdrspub.elastic.elastic_utils import es_delete_index, es_create_index, parse_yaml_params, es_put_resource, \
    es_refresh_index, es_get_instance
from omtdrspub.elastic.test import test_elastic_mapping

CONFIG_FILE = "resources/dit_elsevier_meta.yaml"


def compose_uri(path, res_dir, prefix):
    path = os.path.relpath(path, res_dir)
    uri = urljoin(prefix, defaults.sanitize_url_path(path))
    return uri


class TestElasticResourceList(unittest.TestCase):

    config = parse_yaml_params(CONFIG_FILE)
    es = None

    @classmethod
    def setUpClass(cls):
        cls.es = es_get_instance(cls.config.elastic_host, cls.config.elastic_port)
        es_delete_index(cls.es, index=cls.config.elastic_index)
        es_create_index(cls.es, index=cls.config.elastic_index,
                        mapping=test_elastic_mapping.elastic_mapping(cls.config.elastic_resource_type, cls.config.elastic_change_type))
        es_refresh_index(cls.es, index=cls.config.elastic_index)

        print(es_put_resource(cls.es, cls.config.elastic_index, cls.config.elastic_resource_type,
                        "file1", "/test/path/file1.txt", "elsevier",
                              "metadata", 5, "md5", "text/plain", "2017-02-03T12:25:00Z",
                              [{"href": "file1.pdf", "rel": "describes", "mime": "application/pdf"}]))
        print(es_put_resource(cls.es, cls.config.elastic_index,
                        cls.config.elastic_resource_type,
                        "file2", "/test/path/file2.txt", "elsevier",
                              "metadata", 6, "md5", "text/plain", "2017-02-03T12:27:00Z",
                              [{"href": "file2.pdf",    "rel": "describes", "mime": "application/pdf"}]))

        es_refresh_index(cls.es, index=cls.config.elastic_index)

    @classmethod
    def tearDownClass(cls):
        es_delete_index(cls.es, index=cls.config.elastic_index)
        print("Remove index")
        tmp_dir = cls.config.resource_dir
        shutil.rmtree(path=tmp_dir)
        print("Cleanup folder")

    def test_elastic_resourcelist(self):
        eg = ElasticGenerator(config=self.config)
        result = eg.generate_resourcelist()
        resourcelist = result[0] if result[0].capability_name == "resourcelist" else None
        self.assertEqual(resourcelist.resource_count, 2)

if __name__ == '__main__':
    unittest.main()
