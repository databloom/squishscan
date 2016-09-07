#!/usr/local/bin/python

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

client = Elasticsearch()

s = Search(using=client, index="testfsobj") \
    .filter("term", category="search") \
    .query("match", magicident="RIFF (little-endian) data, AVI, 672 x 288, 25.00 fps, video: XviD")   \
#    .query(~Q("match", description="beta"))

s.aggs.bucket('ar_tag', 'terms', field='tags') \
    .metric('max_lines', 'max', field='lines')

response = s.execute()

for hit in response:
    print(hit.meta.score, hit.title)

for tag in response.aggregations.per_tag.buckets:
    print(tag.key, tag.max_lines.value)
