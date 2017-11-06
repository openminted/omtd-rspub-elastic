omtd-rspub-elastic
======================================

**NOTE: this project has been integrated into the [py-resourcesync](https://github.com/resourcesync/py-resourcesync) library.**

An extension for the rspub-core library supporting Elasticsearch storage.

Proposal and documentation available [here](docs/).

## Overview

This software is based on the [rspub-core](https://github.com/EHRI/rspub-core) library, which allows ResourceSync document
generation from resources stored on the file system. This approach can be challenging when dealing with a huge amount 
of resources, since it is necessary to scan the file system multiple times in order to detect changes and regenerate
sitemaps overtime. 

Therefore, we extended the rspub-core library in order to support data storage in Elasticsearch. The proposed approach
is extensively described in the [documentation](docs/). The [protocol](docs/protocol.md) document describes the mappings
used to store resources and changes into an Elasticsearch index. The [description](docs/resourcesync.adoc) document 
provides on overview on the general approach and project goals.

## Usage

The [```ElasticGenerator```](omtdrspub/elastic/elastic_generator.py) takes a configuration dictionary defined
in the [```ElasticRsParameters```](omtdrspub/elastic/es_rs_paras.py) class, which extends the set of parameters
required by the rspub-core [```RsParameters```](https://github.com/EHRI/rspub-core/blob/master/rspub/core/rs_paras.py) class
to properly configure and query an Elasticsearch instance for the ResourceSync framework. Here is an example of configuration file:

```
resource_set: capabilityname
resource_dir: tmp/dit 
metadata_dir: resourcesync/capabilityname
res_root_dir: tmp/dit
url_prefix: http://example.com/
max_items_in_list: 50000
zero_fill_filename: 4
is_saving_pretty_xml: True
is_saving_sitemaps: True
has_wellknown_at_root: True
description_dir: tmp/dit/resourcesync
elastic_host: localhost
elastic_port: 9200
elastic_index: test-resourcesync
elastic_resource_type: resource
elastic_change_type: change
```

_TODO: provide explaination for each parameter_ 

Three executors are provided:
* ```generate_resourcelist```: generates a resourcelist based on the documents stored at the specified ```elastic_resource_type```
* ```generate_new_changelist```: generates a new changelist based on the documents stored at the specified  ```elastic_change_type```
* ```generate_inc_changelist```: updates a previously generated changelist

Each executor will generate ResourceSync-compliant documents for the capability list specified in the configuration.



