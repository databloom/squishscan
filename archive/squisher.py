#!/usr/local/bin/python

import magic
import json
from json import JSONDecoder
import os
import elasticsearch
#import bz2
import gzip
import shutil




es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

es.indices.delete(index='testfsobj', ignore=[400, 404])


meta = {} 
mymetapieces = []
gzipcompressedsize = 0

for base, subdirs, files in os.walk('/dlink'):
    for name in files:
        if name.endswith('.avi'):
	    print("files:", os.path.join(base, name))
	    fullfilepath = os.path.join(base, name)
            mymagicstring = magic.from_buffer(open(fullfilepath).read(1024)) 
	    filesize = os.path.getsize(os.path.join(base, name))
	    #mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize} 
	    meta['path'] = os.path.join(base, name)
            #es.index(index='testfsobj', doc_type='message', id=meta['path'], body=mymetapieces)
	    print ("Found ",fullfilepath,json.dumps(mymetapieces))
	    gzipcompressname = os.path.join(base, name + ".compressiontest.gzip")
	    with open(fullfilepath , 'rb') as f_in, gzip.open(gzipcompressname, 'wb') as f_out:
    		shutil.copyfileobj(f_in, f_out)
	    gzipcompressedsize = os.path.getsize(gzipcompressname)	
	    print("Compressing ", gzipcompressname)
	    mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize, 'gzipcompressedsize' : gzipcompressedsize} 
            es.index(index='testfsobj', doc_type='message', id=meta['path'], body=mymetapieces)
	    print("Indexing files and uploading compression numbers for: ", gzipcompressname)
	    os.remove(gzipcompressname)
	    print("Removed temp file ",gzipcompressname)

		
