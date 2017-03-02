import os
import urllib.parse

import validators
import yaml
from rspub.core.rs_paras import RsParameters, WELL_KNOWN_URL
from rspub.util import defaults


class ElasticRsParameters(RsParameters):
    def __init__(self, **kwargs):
        super(ElasticRsParameters, self).__init__(**kwargs)
        self.resource_set = kwargs['resource_set']
        self.res_root_dir = kwargs['res_root_dir']
        self.elastic_host = kwargs['elastic_host']
        self.elastic_port = kwargs['elastic_port']
        self.elastic_index = kwargs['elastic_index']
        self.elastic_resource_doc_type = kwargs['elastic_resource_doc_type']
        self.elastic_change_doc_type = kwargs['elastic_change_doc_type']

    # def abs_metadata_dir(self) -> str:
    #     """
    #     ``derived`` :samp:`The absolute path to metadata directory`
    #     :return: absolute path to metadata directory
    #     """
    #     return self.metadata_dir
    #
    # @property
    # def metadata_dir(self):
    #     return self._metadata_dir
    #
    # @metadata_dir.setter
    # def metadata_dir(self, path):
    #     if not os.path.isabs(path):
    #         path = os.path.join(self.resource_dir, path)
    #
    #     self._metadata_dir = path

    @property
    def url_prefix(self):
        return self._url_prefix

    def description_url(self):
        """
        ``derived`` :samp:`The current description url`

        The current description url either points to ``{server root}/.well-known/resourcesync``
        or to a file in the metadata directory.

        :return: current description url

        See also: :func:`has_wellknown_at_root`
        """
        if self.has_wellknown_at_root:
            # r = urllib.parse.urlsplit(self.url_prefix)
            # return urllib.parse.urlunsplit([r[0], r[1], WELL_KNOWN_URL, "", ""])
            return urllib.parse.urljoin(self.url_prefix, WELL_KNOWN_URL)
        else:
            path = self.abs_metadata_path(WELL_KNOWN_URL)
            rel_path = os.path.relpath(path, self.resource_dir)
            return self.url_prefix + defaults.sanitize_url_path(rel_path)

    @url_prefix.setter
    def url_prefix(self, value):
        if value.endswith("/"):
            value = value[:-1]
        parts = urllib.parse.urlparse(value)
        if parts[0] not in ["http", "https"]:  # scheme
            raise ValueError("URL schemes allowed are 'http' or 'https'. Given: '%s'" % value)
        is_valid_domain = validators.domain(parts.hostname)  # hostname

        if parts.port is None:
            is_valid_port = True

        else:
            is_valid_port = is_int(parts.port)

        if not is_valid_domain:
            raise ValueError("URL has invalid domain name: '%s'. Given: '%s'" % (parts.hostname, value))
        if not is_valid_port:
            raise ValueError("URL has invalid port: '%s'. Given: '%s'" % (parts.port, value))
        if parts[4] != "":  # query
            raise ValueError("URL should not have a query string. Given: '%s'" % value)
        if parts[5] != "":  # fragment
            raise ValueError("URL should not have a fragment. Given: '%s'" % value)
        is_valid_url = validators.url(value)
        if not is_valid_url:
            raise ValueError("URL is invalid. Given: '%s'" % value)
        if not value.endswith("/"):
            value += "/"
        self._url_prefix = value

    @staticmethod
    def from_yaml_params(config_file):

        f = open(config_file, 'r+')
        config = yaml.load(f)['executor']

        if not os.path.exists(config['description_dir']):
            os.makedirs(config['description_dir'])

        rs_params = ElasticRsParameters(**config)
        return rs_params


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False
