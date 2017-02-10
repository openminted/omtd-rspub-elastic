import optparse
import os
from concurrent.futures import ThreadPoolExecutor
from os.path import basename

import multiprocessing
import yaml
from elasticsearch import Elasticsearch
from rspub.util import defaults

limit = -1


def create_index(host, port, index, resource_type, change_type):
    es = Elasticsearch([{"host": host, "port": port}])
    # todo: make documents unique for each file
    mapping = {
        "mappings": {
            resource_type: {
                "_timestamp": {
                    "enabled": "true",
                    "format": "basic_date_time_no_millis",
                    "store": "yes",
                },
                "properties": {
                    "rel_path": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "length": {
                        "type": "integer",
                        "index": "not_analyzed"
                    },
                    "md5": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "mime": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "lastmod": {
                        "type": "date",
                    },
                    "res_set": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "res_type": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "ln": {
                        "type": "nested",
                        "index_name": "link",
                        "properties": {
                            "rel": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "href": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "mime": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    }
                }
            },
            change_type: {
                "_timestamp": {
                    "enabled": "true",
                    "format": "basic_date_time_no_millis",
                    "store": "yes"
                },
                "properties": {
                    "rel_path": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "lastmod": {
                        "type": "date",
                    },
                    "change": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "res_set": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "res_type": {
                        "type": "string",
                        "index": "not_analyzed"
                    }
                }
            }
        }
    }
    return es.indices.create(index=index, body=mapping, ignore=400)


def put_into_elasticsearch(elastic_host, elastic_port, elastic_index, elastic_resource_type, res_set, res_type, abs_path, rel_path, ln,
                           ):
    stat = os.stat(abs_path)
    doc = {
        "rel_path": rel_path,
        "length": stat.st_size,
        "md5": defaults.md5_for_file(abs_path),
        "mime": defaults.mime_type(abs_path),
        "lastmod": defaults.w3c_datetime(stat.st_ctime),
        "res_set": res_set,
        "res_type": res_type,
        "ln": ln
    }

    es = Elasticsearch([{"host": elastic_host, "port": elastic_port}])
    return es.index(index=elastic_index, doc_type=elastic_resource_type, body=doc,
                    id=os.path.basename(abs_path).replace(".", ""))


def traverse_folder(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type, pub_folder, root_dir):
    global limit
    count = 0
    cur_folder = pub_folder
    files_names = os.listdir(cur_folder)
    for f in files_names:
        if limit < 0 or (limit > 0 and count < limit):
            abs_path = os.path.join(cur_folder, f)
            if os.path.isdir(abs_path):
                traverse_folder(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type,
                                abs_path, root_dir)
            else:
                if not basename(abs_path).startswith('.'):
                    ln_mime, ln_href, ln_rel = "", "", ""
                    # todo: substitute with es bulk API
                    if res_type == "metadata":
                        ln_mime = "application/pdf"
                        ln_href = abs_path.replace("/metadata/", "/pdf/", 1).replace(".xml", ".pdf")
                        ln_rel = "describes"
                    elif res_type == "pdf":
                        ln_mime = "text/xml"
                        ln_href = abs_path.replace("/pdf/", "/metadata/", 1).replace(".pdf", ".xml")
                        ln_rel = "describedBy"

                    if os.path.exists(ln_href):
                        ln = [{"href": ln_href, "rel": ln_rel, "mime": ln_mime}]
                    else:
                        ln = []

                    rel_path = os.path.relpath(abs_path, root_dir)

                    result = put_into_elasticsearch(elastic_host, elastic_port, elastic_index, elastic_resource_type,
                                                    pub_name, res_type, abs_path, rel_path, ln)
                    print(result)
                    count += 1
        else:
            break


def main():
    parser = optparse.OptionParser()
    parser.add_option('--config-file', '-c',
                      help="populator configuration file")

    # Parse command line arguments
    (args, clargs) = parser.parse_args()

    if len(clargs) > 0:
        parser.print_help()
        return
    if args.config_file is None:
        parser.print_help()
        return

    config = yaml.load(open(args.config_file, 'r'))['populator']
    res_sets = config['res_sets']
    res_root_dir = config['res_root_dir']
    elastic_host = config['elastic_host']
    elastic_port = config['elastic_port']
    elastic_index = config['elastic_index']
    elastic_resource_type = config['elastic_resource_type']
    elastic_change_type = config['elastic_change_type']
    global limit
    if 'limit' in config:
        limit = config['limit']

    executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())

    result = create_index(elastic_host, elastic_port, elastic_index, elastic_resource_type, elastic_change_type)
    print(result)
    for res_set in res_sets:
        subfolders = res_set['subfolders']
        for subfolder in subfolders:
            if 'type' in res_set:
                folder_type = res_set['type']
            else:
                folder_type = subfolder
            executor.submit(traverse_folder, elastic_host, elastic_port, elastic_index, elastic_resource_type,
                            res_set["name"], folder_type, os.path.join(res_set["resources"], subfolder), res_root_dir)


if __name__ == '__main__':
    main()
