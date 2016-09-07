#!/usr/local/bin/python

from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

es.indices.refresh(index="testfsobj")

res = es.search(index="testfsobj", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
    print("%(path)s %(filesize)s: %(magicident)s" % hit["_source"])
