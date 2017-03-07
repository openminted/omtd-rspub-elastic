import unittest

from omtdrspub.elastic.elastic_query_manager import ElasticQueryManager, DuplicateResourceException, \
    ResourceAlreadyExistsException
from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.model.location import Location
from omtdrspub.elastic.model.resource_doc import ResourceDoc
from omtdrspub.elastic.test import test_elastic_mapping

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
                            mapping=test_elastic_mapping.elastic_mapping(cls.resource_doc_type,
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
        cls.qm.index_resource(index=cls.index, resource_doc_type=cls.resource_doc_type,
                              resource_doc=res_doc1)
        cls.qm.index_resource(index=cls.index, resource_doc_type=cls.resource_doc_type,
                              resource_doc=res_doc2)

        cls.qm.refresh_index(index=cls.index)

    @classmethod
    def tearDownClass(cls):
        cls.qm.delete_index(index=cls.index)

    def test_resource_by_location(self):
        result = self.qm.get_resource_by_location(index=self.index, resource_set='elsevier',
                                                  doc_type=self.resource_doc_type,
                                                  location=Location(loc_type="abs_path", value="/test/path/file1.txt"))

        self.assertTrue(result.location.value == "/test/path/file1.txt" and result.location.loc_type == "abs_path")

    def test_resource_duplicates_detection(self):
        res_doc1 = ResourceDoc(location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                               resource_set="elsevier",
                               length=5,
                               md5="md5:",
                               mime="text/plain",
                               lastmod="2017-02-03T12:25:00Z", resync_id="1")

        new_id = self.qm.index_resource(index=self.index, resource_doc_type=self.resource_doc_type,
                                        resource_doc=res_doc1)['_id']
        self.qm.refresh_index(index=self.index)
        self.assertRaises(DuplicateResourceException,
                          self.qm.get_resource_by_resync_id, index=self.index,
                          doc_type=self.resource_doc_type,
                          resource_set=res_doc1.resource_set, resync_id="1"
                          )

        self.assertRaises(DuplicateResourceException,
                          self.qm.get_resource_by_location, index=self.index,
                          doc_type=self.resource_doc_type,
                          resource_set=res_doc1.resource_set,
                          location=res_doc1.location
                          )
        self.qm.delete_document(index=self.index, doc_type=self.resource_doc_type, elastic_id=new_id)
        self.qm.refresh_index(index=self.index)

    def test_resource_duplicates_on_creation(self):

        self.assertRaises(ResourceAlreadyExistsException, self.qm.create_resource, self.config,
                          location=Location(loc_type="abs_path", value="/test/path/file1.txt"),
                          length=5,
                          md5="md5:",
                          mime="text/plain",
                          lastmod="2017-02-03T12:25:00Z", resync_id="1", record_change=False)
