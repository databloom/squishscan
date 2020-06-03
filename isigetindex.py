from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd
import subprocess
import json
import time

#mounts = {}
#for line in subprocess.check_output(['mount', '-l']).split('\n'):
#   parts = line.split(' ')
#    if len(parts) > 2:
#        mounts[parts[2]] = parts[0]
#
#print(mounts)
mountcounter=0
mymountarray =[]

df = pd.read_csv("/proc/mounts", sep="\s+" )

#for mymounts in df.iterrows():
    #if nfs in mymounts:
        #print(mymounts)

#print (df.head())
#print (df.columns)



client = Elasticsearch("http://10.231.154.121:9200")
s = Search(using=client, index="isi")

recdf = pd.DataFrame([hit.to_dict() for hit in s.scan()])
es = Elasticsearch("http://10.231.154.121:9200/")
es.indices.delete(index='onefs-isi-get', ignore=[400, 404])
es.indices.create(index='onefs-isi-get', ignore=[400, 404])
es.indices.delete(index='onefs-isi-pg', ignore=[400, 404])
es.indices.create(index='onefs-isi-pg', ignore=[400, 404])



for index, fileinfo in recdf.iterrows():
    ts=time.time()
    #print(fileinfo['path'])
    ifspath=fileinfo['path']
    print("ifs path:", ifspath.replace("/ifs/","isi get -DD /ifs/data/"))
    sshpath=ifspath.replace("/gonzo/", "isi get -DD /ifs/data/")

    process = subprocess.Popen(['ssh', 'root@172.16.1.1', sshpath],stdout=subprocess.PIPE,universal_newlines=True)
    #result = {}
    #for row in process.stdout.readline():
    #   print("a row entry", row)
    #   if ': ' in row:
    #       key, value = row.split(': ')
    #       print("A key", key)
    #       print("A value", value)
    #       result[key.strip(' .')] = value.strip()
    #print(result)
    #print("output string section ########################")
    #print("Specific key", result['IFS inode'])
    #print("Specific key", result['LIN'])
    mycommandoutput=""
    PG=""
    countPGgroup=0
    insidePG = False
    isi_get = { }
    isi_pg = { }
    while True:
        output = process.stdout.readline()
        #print("--->",output.strip())
        mycommandoutputraw = output.strip()
        #Look for simple key/valuepairs
        if '* ' in mycommandoutputraw:
            mycommandoutput = mycommandoutputraw.replace("* ", "")
            if ": " in mycommandoutput:
                prekey, prevalue = mycommandoutput.split(': ' )
                key = prekey.strip()
                value = prevalue.strip()
                #print("A key", key)
                #print("A value", value)
                #print("simple keys/pairs", key, value)
                isi_get[key] = value


        # Do something else
        # Look for

        if "Metatree logical blocks:" in output:
            te_PG=time.time()
            print("PG seialization time ",fileinfo['path'], te_PG-ts_PG)

            insidePG=False
            #print("Now leaving the protection group block")
         

        if insidePG is True:
            if not output.isspace():
                countPGgroup=countPGgroup+1
                PG = PG + output

                #:y1
                isi_pg[countPGgroup]=output.strip()
                #print("PG Fragment", output, countPGgroup, insidePG, len(output))
        if "PROTECTION GROUPS" in output:
            ts_PG=time.time()
            insidePG = True
            # print("Oh look a protection group")

        #print("these are the protection groups as a blob:", PG)

        return_code = process.poll()
        if return_code is not None:
            print('RETURN CODE', return_code)
            # Process has finished, read rest of the output
            myisipg=""
            #myisipg = ' '.join([str(elem) for elem in isi_pg])
            ts_PG_upload=time.time()
            myisipg=json.dumps(isi_pg)

            #print("example PG",myisipg)
            isi_get['PG'] = myisipg
            es.index(index='onefs-isi-get', doc_type='message', id=fileinfo['path'], body=isi_get)
            te_PG_upload=time.time()
            print("Time for PG json upload to ES",fileinfo['path'], te_PG_upload-ts_PG_upload)
            print("record and file to ES", isi_get, fileinfo['path'])
            #isi_get['PG'] = "PG TEMP" 
            #print("json len", len(myisipg))
            #for output in process.stdout.readlines():
            #     if insidePG is True:
            #         print("--->", output.strip())

            break
         
    te = time.time()
    print("Execution time for file ",fileinfo['path'], te-ts)

    #es = Elasticsearch("http://10.231.154.121:9200/")
    #es.indices.delete(index='onefs-isi-get', ignore=[400, 404])
    #es.indices.create(index='onefs-isi-get', ignore=[400, 404])
    #es.indices.delete(index='onefs-isi-pg', ignore=[400, 404])
    #es.indices.create(index='onefs-isi-pg', ignore=[400, 404])

    #es.index(index='onefs-isi-get', doc_type='docs', id=fileinfo['path'], body=isi_get)
    #es.index(index='onefs-isi-pg', doc_type='docs', id=fileinfo['path'], body=myisipg)

    #print("test of isi-get dump for keyvals", isi_get)
    #print("test of isi-pg dump ", isi_pg)

    #ifspath=fileinfo.replace("/dlink","/ifs/data/")
    #print("ifs path:",ifspath)



#print(df.to_string())




