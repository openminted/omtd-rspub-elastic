#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from rspub.core.rs_enum import Strategy
from omtdrspub.elastic.elastic_rs import ElasticResourceSync


class ElasticGenerator(object):

    def __init__(self, config):
        self.config = config

    def generate(self):
        rs = ElasticResourceSync(**self.config.__dict__)
        return rs.execute()

    def generate_resourcelist(self):
        self.config.strategy = Strategy.resourcelist.value
        return self.generate()

    def generate_new_changelist(self):
        self.config.strategy = Strategy.new_changelist.value
        return self.generate()

    def generate_inc_changelist(self):
        self.config.strategy = Strategy.inc_changelist.value
        return self.generate()

