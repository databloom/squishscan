#!/usr/local/bin/python
import itertools
import multiprocessing
import magic
import json
from json import JSONDecoder
import os
import elasticsearch




es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

#wipe out old index
es.indices.delete(index='testfsobj', ignore=[400, 404])



def main():
    with multiprocessing.Pool(10) as Pool: # pool of 10 processes

        walk = os.walk("/dlink")
        fn_gen = itertools.chain.from_iterable((os.path.join(root, file)
                                                for file in files)
                                               for root, dirs, files in walk)

        results_of_work = pool.map(worker, fn_gen) # this does the parallel processing

	print("Done finall:", results_of_work)



def worker(filename):
	meta = {} 
	mymetapieces = []
    	fullfilepath = os.path.join(base, name)
        mymagicstring = magic.from_buffer(open(fullfilepath).read(1024)) 
	filesize = os.path.getsize(os.path.join(base, name))
	mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize} 
	meta['path'] = os.path.join(base, name)
        es.index(index='testfsobj', doc_type='message', id=meta['path'], body=mymetapieces)
	print ("ES insert complete for",json.dumps(mymetapieces))

