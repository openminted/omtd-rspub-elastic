from glob import glob

from rspub.core.rs import ResourceSync
from rspub.core.rs_enum import Strategy
from rspub.util.observe import Observable

from resyncserver.elastic.exe_elastic_resourcelist import ElasticResourceListExecutor
from resyncserver.server_rs_paras import ServerRsParameters


class ElasticResourceSync(ResourceSync, ServerRsParameters):

    def __init__(self, index, doc_type, **kwargs):
        # super(ElasticResourceSync, self).__init__(**kwargs)
        Observable.__init__(self)
        ServerRsParameters.__init__(self, **kwargs)
        self.index = index
        self.doc_type = doc_type

    def execute(self, filenames: iter=None, start_new=False):
        """
        :samp:`Publish ResourceSync documents under conditions of current {parameters}`

        Call appropriate executor and publish sitemap documents on the resources found in `filenames`.

        If no file/files 'resourcelist_*.xml' are found in metadata directory will always dispatch to
        strategy (new) ``resourcelist``.

        If ``parameter`` :func:`~rspub.core.rs_paras.RsParameters.is_saving_sitemaps` is ``False`` will do
        a dry run: no existing sitemaps will be changed and no new sitemaps will be written to disk.

        :param filenames: filenames and/or directories to scan
        :param start_new: erase metadata directory and create new resourcelists
        """
        # always start fresh publication with resourcelist
        resourcelist_files = sorted(glob(self.abs_metadata_path("resourcelist_*.xml")))
        start_new = start_new or len(resourcelist_files) == 0

        paras = ServerRsParameters(**self.__dict__)
        executor = None

        if self.strategy == Strategy.resourcelist or start_new:
            executor = ElasticResourceListExecutor(paras, index=self.index, doc_type=self.doc_type)
        elif self.strategy == Strategy.new_changelist:
            # todo
            # executor = ElasticNewChangeListExecutor(paras, index=self.index, doc_type=self.doc_type)
            pass
        elif self.strategy == Strategy.inc_changelist:
            # todo
            # executor = ElasticIncrementalChangeListExecutor(paras, index=self.index, doc_type=self.doc_type)
            pass

        if executor:
            executor.register(*self.observers)
            executor.execute()
        else:
            raise NotImplementedError("Strategy not implemented: %s" % self.strategy)

        # # associate current parameters with a selector
        # if isinstance(filenames, Selector):
        #     if filenames.location:
        #         try:
        #             filenames.write()
        #             self.selector_file = filenames.abs_location()
        #             LOG.info("Associated parameters '%s' with selector at '%s'"
        #                      % (self.configuration_name(), self.selector_file))
        #         except Exception as err:
        #             LOG.warning("Unable to save selector: {0}".format(err))

        # set a timestamp
        if self.is_saving_sitemaps:
            self.last_execution = executor.date_start_processing

        self.save_configuration(True)