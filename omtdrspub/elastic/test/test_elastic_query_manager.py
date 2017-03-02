import shutil
import unittest

from omtdrspub.elastic.elastic_query_manager import ElasticQueryManager
from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.location import Location
from omtdrspub.elastic.model.resource_doc import ResourceDoc
from omtdrspub.elastic.test import test_elastic_mapping

CONFIG_FILE = "resources/dit_elsevier_meta.yaml"


class TestElasticQueryManager(unittest.TestCase):
    config = ElasticRsParameters.from_yaml_params(CONFIG_FILE)
    qm: ElasticQueryManager = None

    @classmethod
    def setUpClass(cls):
        cls.qm = ElasticQueryManager(cls.config.elastic_host, cls.config.elastic_port)
        cls.qm.delete_index(index=cls.config.elastic_index)
        cls.qm.create_index(index=cls.config.elastic_index,
                            mapping=test_elastic_mapping.elastic_mapping(cls.config.elastic_resource_doc_type,
                                                                         cls.config.elastic_change_doc_type))

        res_doc1 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               lastmod="2017-02-03T12:25:00Z")

        res_doc2 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file2.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               lastmod="2017-02-03T12:27:00Z")
        cls.qm.index_resource(index=cls.config.elastic_index, resource_doc_type=cls.config.elastic_resource_doc_type,
                              resource_doc=res_doc1)
        cls.qm.index_resource(index=cls.config.elastic_index, resource_doc_type=cls.config.elastic_resource_doc_type,
                              resource_doc=res_doc2)

        cls.qm.refresh_index(index=cls.config.elastic_index)

    @classmethod
    def tearDownClass(cls):
        cls.qm.delete_index(index=cls.config.elastic_index)

    def test_resource_by_location(self):
        result = self.qm.get_resource_by_location(index=self.config.elastic_index, resource_set='elsevier',
                                                  doc_type=self.config.elastic_resource_doc_type,
                                                  location=Location(loc_type="abs_path", value="/test/path/file1.txt"))

        self.assertTrue(len(result) == 1)
