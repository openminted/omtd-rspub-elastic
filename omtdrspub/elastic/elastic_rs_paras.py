import urllib.parse

import validators
from rspub.core.rs_paras import RsParameters


class ElasticRsParameters(RsParameters):

    def __init__(self, **kwargs):
        super(ElasticRsParameters, self).__init__(**kwargs)
        self.publisher_name = kwargs['publisher_name']
        self.res_type = kwargs['res_type']
        self.elastic_host = kwargs['elastic_host']
        self.elastic_port = kwargs['elastic_port']
        self.elastic_index = kwargs['elastic_index']
        self.elastic_resource_type = kwargs['elastic_resource_type']
        self.elastic_change_type = kwargs['elastic_change_type']

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

    @url_prefix.setter
    def url_prefix(self, value):
        if value.endswith("/"):
            value = value[:-1]
        parts = urllib.parse.urlparse(value)
        if parts[0] not in ["http", "https"]:  # scheme
            raise ValueError("URL schemes allowed are 'http' or 'https'. Given: '%s'" % value)
        is_valid_domain = validators.domain(parts.hostname)  #hostname

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


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


class ElasticRsChangelistParameters(ElasticRsParameters):

    def __init__(self, **kwargs):
        super(ElasticRsParameters, self).__init__(**kwargs)
        self.changes_since = kwargs['changes_since']



