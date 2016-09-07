#!/usr/local/bin/python

import magic
import json
from json import JSONDecoder
import os
import elasticsearch
#import bz2
import gzip
import shutil
from threading import Thread
import multiprocessing
import psutil


es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

es.indices.delete(index='testfsobj', ignore=[400, 404])


alist_filter = ['123','3dm','3ds','3g2','3gp','7z','Alt','Int','MIM','The','Thi','aab','aac','aam','aas','abw','ac','acc','ace','acu','adp','aep','afp','ahe','ai','aif','air','ait','ami','and','apk','app','apr','arc','asc','asf','aso','atc','ato','atx','au','aud','avi','aw','azf','azs','azw','bcp','bdf','bdm','bed','bh2','bin','blb','bmi','bmp','box','bti','bz','bz2','c','c11','c4g','cab','caf','car','cat','cbr','ccx','cdb','cdk','cdm','cdx','cdy','cer','cfs','cgm','cha','che','chm','chr','cif','cii','cil','cla','clk','clp','cmc','cmd','cml','cmp','cmx','cod','con','cpi','cpt','crd','crl','cry','csh','csm','csp','css','csv','cu','cur','cww','dae','daf','dar','dav','dbk','dcu','dd2','ddd','deb','der','dfa','dgc','dir','dis','djv','dmg','dna','doc','dot','dp','dpg','dra','dsc','dss','dtb','dtd','dts','dvb','dvi','dwf','dwg','dxf','dxp','ece','ecm','edm','edx','efi','ei6','eml','emm','eol','eot','epu','es3','esa','esf','etx','eva','evy','exe','exi','ext','ez','ez2','ez3','f','f4v','fbs','fcd','fcs','fdf','fe_','fg5','fh','fig','fla','fli','flo','flv','flw','flx','fly','fm','fnc','fpx','fsc','fst','ftc','fti','fvt','fxp','fzs','g2w','g3','g3w','gac','gam','gbr','gca','gdl','geo','gex','ggb','ggt','ghf','gif','gim','gml','gmx','gnu','gph','gpx','gqf','gra','grv','grx','gsf','gta','gtm','gtw','gv','gxf','gxt','h26','hal','hbc','hdf','her','hlp','hpg','hpi','hps','hqx','htk','htm','hvd','hvp','hvs','i2g','icc','ice','ico','ics','ide','ief','ifm','igl','igm','igs','igx','iif','ima','imp','ims','ink','ins','iot','ipf','ipk','irm','irp','iso','itp','ivp','ivu','jad','jam','jar','jav','jis','jlt','jnl','jod','jpe','jpg','jpm','js','jso','kar','kfo','kia','kml','kmz','kne','kon','kpr','kpx','ksp','ktx','ktz','kwd','las','lat','lbd','lbe','les','lin','lnk','los','lrm','ltf','lvp','lwp','lzh','m21','m3u','m4a','m4v','ma','mad','mag','mat','mbk','mbo','mc1','mcd','mcu','mdb','mdi','mes','met','mfm','mft','mgp','mgz','mid','mie','mif','mj2','mka','mkv','mlp','mmd','mmf','mmr','mng','mny','mod','mov','mp4','mpc','mpe','mpg','mpk','mpm','mpn','mpp','mpy','mqy','mrc','msc','mse','msf','msh','msl','mst','mts','mul','mus','mvb','mwf','mxf','mxl','mxm','mxs','mxu','n-g','n3','nbp','nc','ncx','nfo','ngd','nlu','nml','nnd','nns','nnw','npx','nsc','nsf','ntf','nzb','oa2','oa3','oas','obd','obj','oda','odb','odc','odf','odg','odi','odm','odp','ods','odt','oga','ogv','ogx','omd','one','opf','opm','org','osf','otc','otf','otg','oth','oti','otp','ots','ott','oxp','oxt','p','p10','p12','p7b','p7m','p7r','p7s','p8','paw','pbd','pbm','pca','pcf','pcl','pcu','pcx','pdb','pdf','pfa','pfr','pgm','pgn','pgp','pic','pki','plb','plc','plf','pls','pml','png','pnm','por','pot','ppa','ppd','ppm','pps','ppt','prc','pre','prf','psb','psd','psf','psk','pti','pub','pvb','pwn','pya','pyv','qam','qbo','qfx','qps','qt','qxd','ram','rar','ras','rcp','rdf','rdz','rep','res','rgb','rif','rip','ris','rl','rlc','rld','rm','rmp','rms','rmv','rnc','roa','rp9','rps','rq','rs','rsd','rss','rtf','rtx','s','s3m','saf','sbm','sc','scd','scm','scq','scs','scu','sda','sdc','sdd','sdk','sdp','sdw','see','sem','ser','set','sfd','sfs','sfv','sgi','sgl','sgm','sh','sha','shf','sid','sil','sis','sit','skp','sld','slt','sm','smf','smi','smv','smz','snf','spf','spl','spo','spp','spq','sql','src','srt','sru','srx','ssd','sse','ssf','ssm','st','stc','std','stf','sti','stk','stl','str','stw','sub','sus','sv4','svc','svd','svg','swf','swi','sxc','sxd','sxg','sxi','sxm','sxw','t','t3','tag','tao','tar','tca','tcl','tea','tei','tex','tfi','tfm','tga','thm','tif','tmo','tor','tpl','tpt','tra','trm','tsd','tsv','ttf','ttl','twd','txd','txf','txt','ufd','ulx','umj','uni','uom','uri','ust','utz','uu','uva','uvf','uvh','uvi','uvm','uvp','uvs','uvt','uvu','uvv','uvx','uvz','vca','vcd','vcf','vcg','vcs','vcx','vid','vis','viv','vob','vsd','vsf','vtu','vxm','wad','wav','wax','wbm','wbs','wbx','wdp','web','wg','wgt','wm','wma','wmd','wmf','wml','wmv','wmx','wmz','wof','wpd','wpl','wps','wqd','wri','wrl','wsd','wsp','wtb','wvx','x3d','xam','xap','xar','xba','xbd','xbm','xdf','xdm','xdp','xds','xdw','xen','xer','xfd','xht','xif','xla','xlf','xls','xlt','xm','xml','xo','xop','xpi','xpl','xpm','xpr','xps','xpw','xsl','xsm','xsp','xul','xwd','xyz','xz','yan','yin','z1','zaz','zip','zir','zmm']


meta = {} 
mymetapieces = []
	
def indexcompress(i, base, name):
	gzipcompressedsize = 0
	print("files:", os.path.join(base, name))
        fullfilepath = os.path.join(base, name)
        mymagicstring = magic.from_buffer(open(fullfilepath).read(1024))
        filesize = os.path.getsize(os.path.join(base, name))
        meta['path'] = os.path.join(base, name)
        mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize, 'gzipcompressedsize' : gzipcompressedsize}
        print ("Found thread", i, fullfilepath,json.dumps(mymetapieces))
        gzipcompressname = os.path.join(base, name + ".compressiontest.gzip")
        with open(fullfilepath , 'rb') as f_in, gzip.open(gzipcompressname, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        gzipcompressedsize = os.path.getsize(gzipcompressname)
	spacesavingsgzip = filesize - gzipcompressedsize
	if gzipcompressedsize  > 0:
		compressionratio_gzip = filesize/gzipcompressedsize
	if name[-3:] in alist_filter in file: 
		suffix = name[-3:]
        mymetapieces = {'path': fullfilepath, 'magicident' : mymagicstring, 'filesize' : filesize, 'gzipcompressedsize' : gzipcompressedsize , 'compressionratio_gzip' : compressionratio_gzip , 'spacesavingsgzip' : spacesavingsgzip , 'suffix' : suffix }
        print("Compressing thread ", i, gzipcompressname)
        es.index(index='testfsobj', doc_type='message', id=meta['path'], body=mymetapieces)
        print("Indexing files and uploading compression numbers for: thread ",i,  gzipcompressname)
        os.remove(gzipcompressname)
        print("Removed temp file thread ",i,gzipcompressname)	

def cleanupmygzip(i, base, name):
	gzipcompressname = os.path.join(base, name + ".compressiontest.gzip")
	os.remove(gzipcompressname)	
	print ("Deleted ", gzipcompressname)


if 1:
	for base, subdirs, files in os.walk('/dlink/action'):
    		for name in files:
			if name.endswith('.compressiontest.gzip'):
                                for i in range(psutil.cpu_count()):
                                        t = Thread(target=cleanupmygzip, args=(i, base, name))
                                        t.start()
			else:
				print("Cores available", psutil.cpu_count())
				for i in range(1):
					t = Thread(target=indexcompress, args=(i, base, name))
					t.start()

	


if 1:	

	for base, subdirs, files in os.walk('/dlink/action'):
    		for name in files:
        		if name.endswith('.compressiontest.gzip'):
        			print("Cores available", psutil.cpu_count())
        			for i in range(1):
               				t = Thread(target=cleanupmygzip, args=(i, base, name))
                			t.start()

