import ckanapi
from os import listdir
from os.path import isfile,join,splitext
import sys
import re
import csv
from datetime import date

if (len(sys.argv) != 2):
    print 'No api key supplied'
    sys.exit(0)

apikey = sys.argv[1]

refresh_all = False

ckan = ckanapi.RemoteCKAN('http://127.0.0.1:8080', apikey=apikey)

monthlookup = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

path = '/srv/eis-data/Output/Raw'

todaydate = date.today()
year = str(todaydate.year)
datapath  = path + '/' + year

currentdatatypes = csv.reader(open(path + '/currentdatatypes.csv')).next()
print "Current data types: %s" % currentdatatypes

currentpackages = ckan.action.current_package_list_with_resources()

# Make sure we have a package for each data type
for datatype in currentdatatypes:
    if datatype.lower() not in [p['name'] for p in currentpackages]:
        package = ckan.action.package_create(name=datatype.lower(), title=datatype, owner_org='lancaster-university')
        currentpackages.append(package)

# Make sure we have the special package for the Planon metadata
if 'planonmetadata' not in [p['name'] for p in currentpackages]:
    package = ckan.action.package_create(name='planonmetadata', title='PlanonMetaData', owner_org='lancaster-university')
    currentpackages.append(package)

p = re.compile(r"^(\w*)-(\w*)-(\d*)$")

keyedpackages = {p['name']: p for p in currentpackages}

# Replace the metadata files
for package in currentpackages:
    if package['name'] == 'ems':
        for resource in package['resources']:
            if resource['name'] == 'emsmeta':
                ckan.action.resource_delete(id=resource['id'])
        ckan.action.resource_create(package_id=package['id'], name='emsmeta', upload=open(path + '/Meta/EMSMeta.csv'))
    elif package['name'] == 'bms':
        for resource in package['resources']:
            if resource['name'] in ('bmsdevicemeta','bmsmodulemeta'):
                ckan.action.resource_delete(id=resource['id'])
        ckan.action.resource_create(package_id=package['id'], name='bmsdevicemeta', upload=open(path + '/Meta/BMSDeviceMeta.csv'))
        ckan.action.resource_create(package_id=package['id'], name='bmsmodulemeta', upload=open(path + '/Meta/BMSModuleMeta.csv'))
    elif package['name'] == 'locationevent':
        for resource in package['resources']:
            if resource['name'] == 'locationmeta':
                ckan.action.resource_delete(id=resource['id'])
        ckan.action.resource_create(package_id=package['id'], name='locationmeta', upload=open(path + '/Meta/LocationMeta.csv'))
    elif package['name'] == 'planonmetadata':
        for resource in package['resources']:
            ckan.action.resource_delete(id=resource['id'])
        ckan.action.resource_create(package_id=package['id'], name='planonassets', upload=open(path + '/Meta/PlanonAssets.csv'))
        ckan.action.resource_create(package_id=package['id'], name='planonbuildings', upload=open(path + '/Meta/PlanonBuildings.csv'))
        ckan.action.resource_create(package_id=package['id'], name='planonspaces', upload=open(path + '/Meta/PlanonSpaces.csv'))

if not refresh_all:
    # Now update the current month's package for each datatype
    month = monthlookup[todaydate.month]

    for datatype in currentdatatypes:
        packagename = datatype.lower()
        package = keyedpackages[packagename]
        title = package['title']
        current_resource_name = "%s-%s-%s" % (packagename, month.lower(), year)
        current_exists = False
        fullpath = "%s/%s-%s-%s.csv" % (datapath,datatype,month,year)
        print fullpath
        for resource in package['resources']:
            if resource['name'] == current_resource_name:
                current_exists = True
                ckan.action.resource_delete(id=resource['id'])
                ckan.action.resource_create(package_id=package['id'], name=current_resource_name, upload=open(fullpath))
        if not current_exists:
            ckan.action.resource_create(package_id=package['id'], name=current_resource_name, upload=open(fullpath))
else:
    for filename in listdir(datapath):
        fullpath = join(datapath, filename)
        print fullpath
        if isfile(fullpath):
            print "Pushing %s ..." % filename
                
            (name, ext) = splitext(filename)
            (datatype, month, year) = p.match(name).group(1,2,3)

            packagename = datatype.lower()

            package = keyedpackages[packagename]

            try:
                ckan.action.resource_create(package_id=package['id'], name=name.lower(), upload=open(fullpath))
            except Exception:
                print "Failed to create create resource %s" % name.lower()



