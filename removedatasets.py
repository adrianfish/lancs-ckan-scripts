import ckanapi
import sys

if (len(sys.argv) != 2):
    print 'No api key supplied'
    sys.exit(0)

apikey = sys.argv[1]

ckan = ckanapi.RemoteCKAN('http://127.0.0.1:8080', apikey=apikey)

packages = ckan.action.current_package_list_with_resources()

for package in packages:
    for resource in package['resources']:
        ckan.action.resource_delete(id=resource['id'])
    ckan.action.package_delete(id=package['id'])
