import optparse
import os
from os.path import basename

import yaml
from elasticsearch import Elasticsearch
from rspub.util import defaults

INDEX = "resourcesync"
RESOURCE_TYPE = "resource"


def create_index(index, resource_type):
    es = Elasticsearch()
    mapping = {
                "mappings": {
                    resource_type: {
                      "properties": {
                        "filename": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "size": {
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
                        "time": {
                            "type": "date",
                            "index": "not_analyzed"
                        },
                        "publisher": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                      }
                    }
                }
            }
    es.indices.create(index=index, body=mapping, ignore=400)


def put_into_elasticsearch(index: str, resource_type: str, pub_name, file):
    stat = os.stat(file)
    doc = {
            "filename": file,
            "size": stat.st_size,
            "md5": defaults.md5_for_file(file),
            "mime": defaults.mime_type(file),
            "time": defaults.w3c_datetime(stat.st_ctime),
            "publisher": pub_name,
         }

    es = Elasticsearch()
    es.index(index=index, doc_type=resource_type, body=doc)


def traverse(pub_name, pub_folder):
    cur_folder = pub_folder
    files_names = os.listdir(cur_folder)
    for f in files_names:
        f_path = os.path.join(cur_folder, f)
        if os.path.isdir(f_path):
            traverse(f_path)
        else:
            if not basename(f_path).startswith('.'):
                # todo: substitute with es bulk API
                put_into_elasticsearch(INDEX, RESOURCE_TYPE, pub_name, os.path.join(pub_folder, f_path))
                print(f_path)

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
    publishers = config['publishers']
    subfolders = config['subfolders']

    create_index(INDEX, RESOURCE_TYPE)
    for publisher in publishers:
        for subfolder in subfolders:
            traverse(publisher["name"], os.path.join(publisher["resources"], subfolder))

if __name__ == '__main__':
    main()









