from glob import glob

from rspub.core.rs import ResourceSync
from rspub.core.rs_enum import Strategy
from rspub.util.observe import Observable

from omtdrspub.elastic.elastic_rs_paras import ElasticRsParameters
from omtdrspub.elastic.exe_elastic_changelist import ElasticNewChangeListExecutor, ElasticIncrementalChangeListExecutor
from omtdrspub.elastic.exe_elastic_resourcelist import ElasticResourceListExecutor


class ElasticResourceSync(ResourceSync, ElasticRsParameters):

    def __init__(self, **kwargs):
        # super(ElasticResourceSync, self).__init__(**kwargs)
        Observable.__init__(self)
        ElasticRsParameters.__init__(self, **kwargs)

    def execute(self, filenames: iter=None, start_new=False):
        # filenames is not necessary, we use it only to match the method signature
        # always start fresh publication with resourcelist
        resourcelist_files = sorted(glob(self.abs_metadata_path("resourcelist_*.xml")))
        start_new = start_new or len(resourcelist_files) == 0

        paras = ElasticRsParameters(**self.__dict__)
        executor = None

        if self.strategy == Strategy.resourcelist or start_new:
            executor = ElasticResourceListExecutor(paras)
        elif self.strategy == Strategy.new_changelist:
            executor = ElasticNewChangeListExecutor(paras)
        elif self.strategy == Strategy.inc_changelist:
            executor = ElasticIncrementalChangeListExecutor(paras)

        if executor:
            executor.register(*self.observers)
            result = executor.execute()
        else:
            raise NotImplementedError("Strategy not implemented: %s" % self.strategy)
        return result
        # set a timestamp
        #if self.is_saving_sitemaps:
        #    self.last_execution = executor.date_start_processing

        #self.save_configuration(True)