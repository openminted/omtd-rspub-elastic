def elastic_mapping(resource_type, change_type):
    mapping = {
        "mappings": {
            resource_type: {
                "_timestamp": {
                    "enabled": "true",
                    "format": "basic_date_time_no_millis",
                    "store": "yes",
                },
                "properties": {
                    "location": {
                        "type": "nested",
                        "properties": {
                            "value": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "type": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                        }
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
                    "ln": {
                        "type": "nested",
                        "index_name": "link",
                        "properties": {
                            "rel": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "href": {
                                "type": "nested",
                                "properties": {
                                    "value": {
                                        "type": "string",
                                        "index": "not_analyzed"
                                    },
                                    "type": {
                                        "type": "string",
                                        "index": "not_analyzed"
                                    },
                                }
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
                    "location": {
                        "type": "nested",
                        "properties": {
                            "value": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "type": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                        }
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
                    }
                }
            }
        }
    }
    return mapping
