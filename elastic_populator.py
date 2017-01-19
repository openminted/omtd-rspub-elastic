import os
from os.path import basename

from elasticsearch import Elasticsearch
from rspub.util import defaults

FOLDER = "/Users/giorgio/Desktop/dit/resources/springer"
INDEX = "resourcesync-springer"
RESOURCE_TYPE = "resource"


def create_index(index, resource_type):
    es = Elasticsearch()
    mapping = {
                "mappings": {
                    resource_type: {
                      "properties": {
                        "filename": {
                          "type": "text"
                        },
                        "size": {
                            "type": "integer"
                        },
                        "md5": {
                            "type": "text"
                        },
                        "mime": {
                            "type": "text"
                        },
                        "time": {
                            "type": "date"
                        }
                      }
                    }
                }
            }
    es.indices.create(index=index, body=mapping, ignore=400)


def put_into_elasticsearch(index: str, resource_type: str, file):
    stat = os.stat(file)
    doc = {
            "filename": file,
            "size": stat.st_size,
            "md5": defaults.md5_for_file(file),
            "mime": defaults.mime_type(file),
            "date": defaults.w3c_datetime(stat.st_size)
         }
    es = Elasticsearch()
    es.index(index=index, doc_type=resource_type, body=doc)



def traverse(cur_folder):
    files_names = os.listdir(cur_folder)
    for f in files_names:
        f_path = os.path.join(cur_folder, f)
        if os.path.isdir(f_path):
            traverse(f_path)
        else:
            if not basename(f_path).startswith('.'):
                # todo: substitute with es bulk API
                put_into_elasticsearch(INDEX, RESOURCE_TYPE, os.path.join(FOLDER, f_path))
                print(f_path)

if __name__ == '__main__':

    create_index(INDEX, RESOURCE_TYPE)
    traverse(FOLDER)








