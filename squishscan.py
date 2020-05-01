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


# use default of localhost, port 9200
es = elasticsearch.Elasticsearch("http://10.231.154.121:9200/")

es.indices.delete(index='isi', ignore=[400, 404])


meta = {}
mymetapieces = []


def indexcompress(i, base, name):
    gzipcompressedsize = 0
    print("files:", os.path.join(base, name))
    fullfilepath = os.path.join(base, name)
    #omymagicstring = magic.from_buffer(open(fullfilepath).read(2048),mime=True)
    m=magic.open(magic.MAGIC_NONE)
    m.load
    mymagicstring = m.file(fullfilepath)
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
                    'compressionratio_gzip': compressionratio_gzip, 'spacesavingsgzip': spacesavingsgzip, 'filenamesuffix': filenamesuffix}
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


if 1:
    for base, subdirs, files in os.walk('/f810/cribsbiox.west.isilon.com/datasets/'):
        # for base, subdirs, files in os.walk('testdata'):
        for name in files:
            if name.endswith('.compressiontest.gzip'):
                for i in range(psutil.cpu_count()):
                    t = Thread(target=cleanupmygzip, args=(i, base, name))
                    t.start()
            else:
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
