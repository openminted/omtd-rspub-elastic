import optparse
import os
from concurrent.futures import ThreadPoolExecutor
from os.path import basename

import multiprocessing
import yaml
from elasticsearch import Elasticsearch
from rspub.util import defaults

limit = -1


def create_index(host, port, index, resource_type):
    es = Elasticsearch([{"host": host, "port": port}])
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
                        },
                        "res_type": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                      }
                    }
                }
            }
    es.indices.create(index=index, body=mapping, ignore=400)


def put_into_elasticsearch(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type, file):
    stat = os.stat(file)
    doc = {
            "filename": file,
            "size": stat.st_size,
            "md5": defaults.md5_for_file(file),
            "mime": defaults.mime_type(file),
            "time": defaults.w3c_datetime(stat.st_ctime),
            "publisher": pub_name,
            "res_type": res_type
         }

    es = Elasticsearch([{"host": elastic_host, "port": elastic_port}])
    es.index(index=elastic_index, doc_type=elastic_resource_type, body=doc)


def traverse_folder(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type, pub_folder):
    global limit
    count = 0
    cur_folder = pub_folder
    files_names = os.listdir(cur_folder)
    for f in files_names:
        if limit < 0 or (limit > 0 and count < limit):
            f_path = os.path.join(cur_folder, f)
            if os.path.isdir(f_path):
                traverse_folder(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type, f_path)
            else:
                if not basename(f_path).startswith('.'):
                    # todo: substitute with es bulk API
                    put_into_elasticsearch(elastic_host, elastic_port, elastic_index, elastic_resource_type, pub_name, res_type, os.path.join(pub_folder, f_path))
                    print(f_path)
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
    publishers = config['publishers']
    elastic_host = config['elastic_host']
    elastic_port = config['elastic_port']
    elastic_index = config['elastic_index']
    elastic_resource_type = config['elastic_resource_type']
    global limit
    if 'limit' in config:
        limit = config['limit']

    executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())

    create_index(elastic_host, elastic_port, elastic_index, elastic_resource_type)
    for publisher in publishers:
        subfolders = publisher['subfolders']
        for subfolder in subfolders:
            if 'type' in publisher:
                folder_type = publisher['type']
            else:
                folder_type = subfolder
            executor.submit(traverse_folder, elastic_host, elastic_port, elastic_index, elastic_resource_type, publisher["name"], folder_type, os.path.join(publisher["resources"], subfolder))


if __name__ == '__main__':
    main()









