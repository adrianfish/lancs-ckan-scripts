import ckanapi
from os import listdir
from os.path import isfile,join,splitext
import sys
import re
import csv

if (len(sys.argv) != 2):
    print 'No api key supplied'
    sys.exit(0)

apikey = sys.argv[1]

path = '/srv/eis-data/Output/Raw'

emsmeta = {}
csvreader = csv.DictReader(open(path + '/Meta/EMSMeta.csv', 'rU'))
for row in csvreader:
    emsmeta[row['id']] = row

bmsmodulemeta = {}
csvreader = csv.DictReader(open(path + '/Meta/BMSModuleMeta.csv', 'rU'))
for row in csvreader:
    bmsmodulemeta[row['module_id']] = row

locationmeta = {}
csvreader = csv.DictReader(open(path + '/Meta/LocationMeta.csv', 'rU'))
for row in csvreader:
    locationmeta[row['planon_id']] = row

p = re.compile(r"^(\w*)-(\w*)-(\d*)$")

ckan = ckanapi.RemoteCKAN('http://127.0.0.1:8080', apikey=apikey)

datapath  = path + '/2016'

currentpackages = ckan.action.package_list()

for filename in listdir(datapath):
    fullpath = join(datapath, filename)
    print fullpath
    if isfile(fullpath):
        print "Pushing %s ..." % filename
            
        (name, ext) = splitext(filename)
        (datatype, month, year) = p.match(name).group(1,2,3)
        tags = [{'name': datatype}, {'name': month}, {'name': year}]

        if datatype == 'EMS':
            csvreader = csv.reader(open(fullpath, 'rU'))
            for row in csvreader:
                if len(row) > 2:
                    metadata_id = row[2]
                    metadata = emsmeta.get(metadata_id, None)
                    if metadata != None:
                        subnet_description = metadata['subnet_description'].replace('/','_')
                        tags.append({'name': subnet_description})

        try:
            if (datatype not in currentpackages):
                package = ckan.action.package_create(datatype, title=datatype, owner_org='lancaster-university', tags=[{'name': datatype}], asset_code='')
            #package = ckan.action.package_create(name=name.lower(), title=filename, owner_org='lancaster-university', tags=tags, asset_code='jeff')
        except Exception:
            print "Failed to create package %s" % name.lower()
        #try:
        #    ckan.action.resource_create(package_id=package['id'], name=name.lower(), upload=open(fullpath))
        #except Exception:
        #    print "Failed to create create resource %s" % name.lower()
