import os
import shutil
import unittest
from urllib.parse import urljoin

from rspub.util import defaults

from omtdrspub.elastic.elastic_generator import ElasticGenerator
from omtdrspub.elastic.elastic_query_manager import ElasticQueryManager
from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.link import Link
from omtdrspub.elastic.model.location import Location
from omtdrspub.elastic.model.resource_doc import ResourceDoc
from omtdrspub.elastic.test import test_elastic_mapping

CONFIG_FILE = "resources/dit_elsevier_meta.yaml"


def compose_uri(path, res_dir, prefix):
    path = os.path.relpath(path, res_dir)
    uri = urljoin(prefix, defaults.sanitize_url_path(path))
    return uri


class TestElasticResourceList(unittest.TestCase):

    config = ElasticRsParameters.from_yaml_params(CONFIG_FILE)
    qm = None

    @classmethod
    def setUpClass(cls):
        cls.qm = ElasticQueryManager(cls.config.elastic_host, cls.config.elastic_port)
        cls.qm.delete_index(index=cls.config.elastic_index)
        cls.qm.create_index(index=cls.config.elastic_index,
                            mapping=test_elastic_mapping.elastic_mapping(cls.config.elastic_resource_doc_type,
                                                                         cls.config.elastic_change_doc_type))
        cls.qm.refresh_index(index=cls.config.elastic_index)

        res_doc1 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               ln=[Link(href=Location(loc_type="rel_path", value="file1.pdf"), rel="describes",
                                        mime="application/pdf")],
                               lastmod="2017-02-03T12:25:00Z")

        res_doc2 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file2.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               ln=[Link(href=Location(loc_type="rel_path", value="file2.pdf"), rel="describes",
                                        mime="application/pdf")],
                               lastmod="2017-02-03T12:27:00Z")

        cls.qm.index_resource(index=cls.config.elastic_index, resource_doc_type=cls.config.elastic_resource_doc_type,
                              resource_doc=res_doc1)
        cls.qm.index_resource(index=cls.config.elastic_index, resource_doc_type=cls.config.elastic_resource_doc_type,
                              resource_doc=res_doc2)

        cls.qm.refresh_index(index=cls.config.elastic_index)

    @classmethod
    def tearDownClass(cls):
        cls.qm.delete_index(index=cls.config.elastic_index)
        tmp_dir = cls.config.resource_dir
        shutil.rmtree(path=tmp_dir)

    def test_elastic_resourcelist(self):
        eg = ElasticGenerator(config=self.config)
        result = eg.generate_resourcelist()
        resourcelist = result[0] if result[0].capability_name == "resourcelist" else None
        self.assertEqual(resourcelist.resource_count, 2)

if __name__ == '__main__':
    unittest.main()
