#!/usr/local/bin/python

import magic
import json
from json import JSONDecoder
import os
import elasticsearch




es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

es.indices.delete(index='testfsobj', ignore=[400, 404])


meta = {} 
mymetapieces = []


for base, subdirs, files in os.walk('/dlink'):
    for name in files:
        if name.endswith('.avi'):
	    #print("files:", os.path.join(base, name))
	    fullfilepath = os.path.join(base, name)
            mymagicstring = magic.from_buffer(open(fullfilepath).read(1024)) 
	    filesize = os.path.getsize(os.path.join(base, name))
	    mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize} 
	    meta['path'] = os.path.join(base, name)
            es.index(index='testfsobj', doc_type='message', id=meta['path'], body=mymetapieces)
	    print ("ES insert complete for",json.dumps(mymetapieces))


