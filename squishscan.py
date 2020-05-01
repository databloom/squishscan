#!/usr/local/bin/python
import time
import magic
import json
from json import JSONDecoder
import os
import elasticsearch
import gzip
import shutil
from threading import Thread
import multiprocessing
import psutil
import threading
import hashlib


# use default of localhost, port 9200
es = elasticsearch.Elasticsearch("http://10.231.154.121:9200/")

es.indices.delete(index='isi', ignore=[400, 404])


meta = {}
mymetapieces = []


def indexcompress(i, base, name):
    with open( os.path.join(base, name),"rb") as f:
        bytes= f.read()
        readable_hash = hashlib.md5(bytes).hexdigest()

    gzipcompressedsize = 0
    print("files:", os.path.join(base, name))
    fullfilepath = os.path.join(base, name)
    #mymagicstring = magic.from_buffer(open(fullfilepath).read(2048),mime=True)
    mymagicstring = magic.from_file(fullfilepath,mime=True)
    #m=magic.open(magic.MAGIC_MIME)
    #m.load
    #mymagicstring = m.file(fullfilepath)
    #mymagicstring = m.file(fullfilepath)
    #mime = magic.Magic(mime=true)
    #mymagicstring = mime.from_file(fullfilepath)
    #print ("my magic string check", fullfilepath, mymagicstring)
    filesize = os.path.getsize(os.path.join(base, name))
    meta['path'] = os.path.join(base, name)
    mymetapieces = {'path': fullfilepath, 'magicident': mymagicstring,
                    'filesize': filesize, 'gzipcompressedsize': gzipcompressedsize}
    #print ("Launched new thread", fullfilepath, json.dumps(mymetapieces))
    #gzipcompressname = os.path.join(base, name + ".compressiontest.gzip")
    gzipcompressname = os.path.join("/mnt/ramdisk", name + ".compressiontest.gzip")
    with open(fullfilepath, 'rb') as f_in, gzip.open(gzipcompressname, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    gzipcompressedsize = os.path.getsize(gzipcompressname)
    spacesavingsgzip = filesize - gzipcompressedsize
    if gzipcompressedsize > 0:
        compressionratio_gzip = filesize/gzipcompressedsize
    filenamesuffix = ""
    filenamesuffix = os.path.splitext(name)[1]
    mymetapieces = {'path': fullfilepath, 'magicident': mymagicstring, 'filesize': filesize, 'gzipcompressedsize': gzipcompressedsize,
            'compressionratio_gzip': compressionratio_gzip, 'spacesavingsgzip': spacesavingsgzip, 'filenamesuffix': filenamesuffix, 'hash': readable_hash}
    print("Compressing thread ", i, gzipcompressname, mymetapieces)
    es.index(index='isi', doc_type='message',
             id=meta['path'], body=mymetapieces)
    print("Indexing files and uploading compression numbers for: thread ",
            i,  gzipcompressname, mymetapieces)
    os.remove(gzipcompressname)
    print("Removed temp file thread ", i, gzipcompressname)


def cleanupmygzip(i, base, name):
    gzipcompressname = os.path.join(base, name + ".compressiontest.gzip")
    os.remove(gzipcompressname)
    print ("Deleted ", gzipcompressname)


def lookup(mylookuphash):
    #put a search in ES for the hash 
    #s = Search(using=client, index="isi") .filter("term", category="search")  .query("match", hash=mylookuphash \ 
    #        s.aggs.bucket('ar_tag', 'terms', field='tags') \
    #                    .metric('max_lines', 'max', field='lines')

    #response = s.execute()
    return count(response)





if 1:
    for base, subdirs, files in os.walk('/f810/cribsbiox.west.isilon.com/datasets/'):
        # for base, subdirs, files in os.walk('testdata'):
        for name in files:
            if name.endswith('.compressiontest.gzip'):
                for i in range(psutil.cpu_count()):
                    t = Thread(target=cleanupmygzip, args=(i, base, name))
                    t.start()
            else:
                with open( os.path.join(base, name),"rb") as f:
                         bytes= f.read()
                         mylookuphash = hashlib.md5(bytes).hexdigest()

                if (1 ):
                    print("Cores available", psutil.cpu_count(), threading.active_count())
                    while (threading.active_count() >  psutil.cpu_count()) : 
                           time.sleep( 10 )
                    for i in range(1):
                        t = Thread(target=indexcompress, args=(i, base, name))
                        t.start()



if 0:

    for base, subdirs, files in os.walk('/f810/cribsbiox.west.isilon.com/datasets/'):
        #	for base, subdirs, files in os.walk('testdata/'):
        for name in files:
            if name.endswith('.compressiontest.gzip'):
                processcntr = threading.Active_Count()
                print("Cores available", psutil.cpu_count(), threading.Active_Count())
                for i in range(1):
                    t = Thread(target=cleanupmygzip, args=(i, base, name))
                    t.start()
