import ckanapi
import csv
import sys

if (len(sys.argv) != 2):
    print 'No api key supplied'
    sys.exit(0)

apikey = sys.argv[1]

codereader = csv.reader(open('building_codes.csv', 'rU'), dialect='excel')
tags = [{'name': row[0]} for row in codereader]
ckan = ckanapi.RemoteCKAN('http://127.0.0.1:8080', apikey=apikey)
ckan.action.vocabulary_update(id='building_codes', tags=tags)
