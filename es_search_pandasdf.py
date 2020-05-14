from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd

client = Elasticsearch("http://acme:9200")
s = Search(using=client, index="isi")

df = pd.DataFrame([hit.to_dict() for hit in s.scan()])

print(df.to_string())


