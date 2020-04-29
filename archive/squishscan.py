#!/usr/local/bin/python

import json
import os
import elasticsearch
import magic


es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

for base, subdirs, files in os.walk('/dlink'):
#for base, subdirs, files in os.walk('testdata/'):
	for name in files:
#		print(os.path.join(base, name))
		fullpathname = os.path.join(base, name)
#       		if name.endswith('.avi'):
#       		if name.endswith('.gz'):
#			print(os.path.join(base, name))
		#	print(magic.from_file((os.path.join(base, name))))
		scannedmagic = magic.from_buffer(open(fullpathname).read(1024))
		print (fullpathname, ":", scannedmagic);
		with open(os.path.join(base, name), 'r') as fp:
            		meta = json.load(fp)
  			meta['filename'] = os.path.join(base, name) 
  			meta['tld'] = os.path.split(root)[-1]
        		meta['path'] = os.path.join(base, name)
			meta['magic'] = scannedmagic
           		es.index(index='media', doc_type='fsmetadata', id=meta['filename'], body=meta)

