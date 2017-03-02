import os
from urllib.parse import urljoin

from rspub.util import defaults


class Location(object):

    def __init__(self, value: str, loc_type: str):
        self._value = value
        self._loc_type = loc_type

    @property
    def value(self):
        return self._value

    @property
    def loc_type(self):
        return self._loc_type

    @value.setter
    def value(self, value):
        self._value = value

    @loc_type.setter
    def loc_type(self, loc_type):
        self._loc_type = loc_type

    def uri_from_path(self, para_url_prefix, para_res_root_dir) -> str:
        uri = None
        if self.loc_type == 'url':
            uri = self.value
        elif self.loc_type == 'rel_path':
            uri = urljoin(para_url_prefix, defaults.sanitize_url_path(self.value))
        elif self.loc_type == 'abs_path':
            path = os.path.relpath(self.value, para_res_root_dir)
            uri = para_url_prefix + defaults.sanitize_url_path(path)
        return uri

    @staticmethod
    def as_location(dct):
        return Location(value=dct['value'], loc_type=dct['type'])