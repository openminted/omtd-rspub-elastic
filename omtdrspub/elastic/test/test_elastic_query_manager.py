import unittest

from omtdrspub.elastic import elastic_mapping
from omtdrspub.elastic.elastic_query_manager import ElasticQueryManager
from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.location import Location
from omtdrspub.elastic.model.resource_doc import ResourceDoc

CONFIG_FILE = "resources/dit_elsevier_meta.yaml"


class TestElasticQueryManager(unittest.TestCase):
    config = ElasticRsParameters.from_yaml_params(CONFIG_FILE)
    index = config.elastic_index
    resource_doc_type = config.elastic_resource_doc_type
    change_doc_type = config.elastic_change_doc_type

    qm: ElasticQueryManager = None

    @classmethod
    def setUpClass(cls):
        cls.qm = ElasticQueryManager(cls.config.elastic_host, cls.config.elastic_port)
        cls.qm.delete_index(index=cls.index)
        cls.qm.create_index(index=cls.index,
                            mapping=elastic_mapping.elastic_mapping(cls.resource_doc_type,
                                                                    cls.change_doc_type))

        res_doc1 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               lastmod="2017-02-03T12:25:00Z", resync_id="1")

        res_doc2 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file2.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               lastmod="2017-02-03T12:27:00Z", resync_id="2")
        cls.qm.index_document(index=cls.index, doc_type=cls.resource_doc_type,
                              doc=res_doc1.to_dict(), elastic_id=res_doc1.resync_id)
        cls.qm.index_document(index=cls.index, doc_type=cls.resource_doc_type,
                              doc=res_doc2.to_dict(), elastic_id=res_doc2.resync_id)

        cls.qm.refresh_index(index=cls.index)

    @classmethod
    def tearDownClass(cls):
        cls.qm.delete_index(index=cls.index)

    def test_resource_by_location(self):
        result = self.qm.get_document_by_location(index=self.index, resource_set='elsevier',
                                                  doc_type=self.resource_doc_type,
                                                  location=Location(loc_type="abs_path", value="/test/path/file1.txt"))

        self.assertTrue(result.location.value == "/test/path/file1.txt" and result.location.loc_type == "abs_path")

    def test_resource_create_conflict(self):
        result = self.qm.create_resource(params=self.config,
                                         location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                                         length=5,
                                         md5="md5:",
                                         mime="text/plain",
                                         lastmod="2017-02-03T12:25:00Z", elastic_id="1", record_change=False)
        self.assertTrue(result.get('error') is not None)
        self.assertEqual(result.get('status'), 409)

    def test_resource_update(self):
        result = self.qm.update_resource(params=self.config,
                                         location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                                         length=5,
                                         md5="md5:",
                                         mime="text/plain",
                                         lastmod="2017-02-03T12:25:00Z", elastic_id="1", record_change=False)

        self.assertEqual(result.get('created'), False)
