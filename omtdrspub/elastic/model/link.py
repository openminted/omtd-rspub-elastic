from omtdrspub.elastic.model.location import Location


class Link(object):

    def __init__(self, href: Location, rel: str, mime: str):
        self._href = href
        self._rel = rel
        self._mime = mime

    @property
    def href(self):
        return self._href

    @property
    def rel(self):
        return self._rel

    @property
    def mime(self):
        return self._mime

    @href.setter
    def href(self, href):
        self._href = href

    @staticmethod
    def as_link(dct):
        return Link(href=Location.as_location(dct['href']),
                    rel=dct['rel'],
                    mime=dct['mime'])



