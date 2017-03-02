from typing import List

from omtdrspub.elastic.model.link import Link
from omtdrspub.elastic.model.location import Location


class ResourceDoc(object):
    def __init__(self, resource_set=None,
                 location: Location=None, length: int=None, md5: str=None,
                 mime: str=None, lastmod: str=None, ln: List[Link]=None):
        self._resource_set = resource_set
        self._location = location
        self._length = length
        self._md5 = md5
        self._mime = mime
        self._lastmod = lastmod
        self._ln = ln

    @property
    def resource_set(self):
        return self._resource_set

    @property
    def location(self):
        return self._location

    @property
    def length(self):
        return self._length

    @property
    def md5(self):
        return self._md5

    @property
    def mime(self):
        return self._mime

    @property
    def lastmod(self):
        return self._lastmod

    @property
    def ln(self):
        return self._ln

    @ln.setter
    def ln(self, ln):
        self._ln = ln

    @location.setter
    def location(self, location):
        self._location = location

    @staticmethod
    def as_resource_doc(dct):
        return ResourceDoc(resource_set=dct['resource_set'],
                           location=Location.as_location(dct=dct['location']),
                           length=dct['length'],
                           md5=dct['md5'],
                           mime=dct['mime'],
                           lastmod=dct['lastmod'],
                           ln=[Link.as_link(dct=link) for link in dct['ln']])
