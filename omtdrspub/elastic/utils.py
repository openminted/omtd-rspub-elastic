from datetime import datetime

from resync.sitemap import RS_NS, SitemapParseError, SITEMAP_NS, SitemapIndexError

from xml.etree.ElementTree import parse
from resync.resource_container import ResourceContainer


def parse_xml_without_urls(sm, fh=None, etree=None, resources=None, capability=None,
                          sitemapindex=None):
    """
    This is a modification of the Sitemap.parse_xml method of the resync library
    We need to parse rs:md and rs:ln items, avoiding to parse <url> tags
    """
    if resources is None:
        resources = ResourceContainer()
    if fh is not None:
        etree = parse(fh)
    elif etree is None:
        raise ValueError("Neither fh or etree set")
    # check root element: urlset (for sitemap), sitemapindex or bad
    root_tag = etree.getroot().tag
    resource_tag = None  # will be <url> or <sitemap> depending on type
    sm.parsed_index = None
    if root_tag == '{' + SITEMAP_NS + "}urlset":
        sm.parsed_index = False
        if sitemapindex is not None and sitemapindex:
            raise SitemapIndexError(
                "Got sitemap when expecting sitemapindex", etree)
        resource_tag = '{' + SITEMAP_NS + "}url"
    elif root_tag == '{' + SITEMAP_NS + "}sitemapindex":
        sm.parsed_index = True
        if sitemapindex is not None and not sitemapindex:
            raise SitemapIndexError(
                "Got sitemapindex when expecting sitemap", etree)
        resource_tag = '{' + SITEMAP_NS + "}sitemap"
    else:
        raise SitemapParseError(
            "XML is not sitemap or sitemapindex (root element is <%s>)"
            "" % root_tag)

    # have what we expect, read it
    in_preamble = True
    sm.resources_created = 0
    seen_top_level_md = False
    for e in etree.getroot().getchildren():
        # look for <rs:md> and <rs:ln>, first <url> ends
        # then look for resources in <url> blocks
        if e.tag == resource_tag:
            pass
            # in_preamble = False  # any later rs:md or rs:ln is error
            # r = self.resource_from_etree(e, self.resource_class)
            # try:
            #     resources.add(r)
            # except SitemapDupeError:
            #     self.logger.warning(
            #         "dupe of: %s (lastmod=%s)" % (r.uri, r.lastmod))
            # self.resources_created += 1
        elif e.tag == "{" + RS_NS + "}md":
            if in_preamble:
                if seen_top_level_md:
                    raise SitemapParseError(
                        "Multiple <rs:md> at top level of sitemap")
                else:
                    resources.md = sm.md_from_etree(e, 'preamble')
                    seen_top_level_md = True
            else:
                raise SitemapParseError(
                    "Found <rs:md> after first <url> in sitemap")
        elif e.tag == "{" + RS_NS + "}ln":
            if in_preamble:
                resources.ln.append(sm.ln_from_etree(e, 'preamble'))
            else:
                raise SitemapParseError(
                    "Found <rs:ln> after first <url> in sitemap")
        else:
            # element we don't recognize, ignore
            pass
    # check that we read to right capability document
    if capability is not None:
        if 'capability' not in resources.md:
            if capability == 'resourcelist':
                sm.logger.warning(
                    'No capability specified in sitemap, assuming '
                    'resourcelist')
                resources.md['capability'] = 'resourcelist'
            else:
                raise SitemapParseError(
                    "Expected to read a %s document, but no capability "
                    "specified in sitemap" % capability)
        if resources.md['capability'] != capability:
            raise SitemapParseError("Expected to read a %s document, got %s" % (capability, resources.md['capability']))
    # return the resource container object
    return resources


def formatted_date(d: datetime):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


