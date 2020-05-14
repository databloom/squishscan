#!/usr/local/bin/python

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

es = Elasticsearch("http://10.231.154.121:9200")






res = es.search(index="isi",body={"query": {"match_all": {}}},size=10000)
print("Got %d Hits:" % res['hits']['total']['value'])
myhits = len(res['hits']['hits'])
print(myhits)
for hit in res['hits']['hits']:
        print("%(hash)s: %(path)s" % hit["_source"] )
