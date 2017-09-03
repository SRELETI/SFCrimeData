#!/usr/bin/python
# -*- coding: utf-8 -*-

import os # Library to access the underlying file system
from bottle import route,run,hook,response,request # LightWeight web server to host the api
import simplejson as json # Library to deal with json
import ibm_db
import logging
from operator import itemgetter
# Library to access the Object Data Storage. Object Data Storage is built on Top of Open Stack. Open Stack uses Key Stone for authentication. So, we need it to access Object Data Storage.
import keystoneclient.v2_0.client 
import swiftclient.client as swiftclient # Library to access the Object Data Storage
# By Default, when a call is made by a front end application to an api, the call fails if both the front end application and api is not hosted on the same server(domain). This is because
# browsers enforce same domain policy. They dont allow cross domain calls. However, there are many ways in which we can make this work. Making it to work is not violating any best practices.
# One easy way to make it work is by adding the below three values in the response headers. 
# 'Access-Control-Allow-Origin' value tells the client which is requesting data from the api about the allowed servers which can make a request. '*' means all.
# 'Access-Control-Allow-Origin' value tells the client which request methods are valid.
# 'Access-Control-Allow-Origin' value tells the client which request headers can be present. 
# The browsers when they make a request to a api with a get/post/put/delete method first they make a 'options' request and the hook('after_request') annotation will make sure that after every request the method enable_cors is called and the response sent from the api for 'options' request will contain the above headers.
# Using the information from headers, the browsers will confirm that they can make a cross domain request and then send a get/post/put/delete request.  
_allow_origin = '*'
_allow_methods = 'PUT,GET,POST,DELETE,OPTIONS'
_allow_headers = 'Authorization, Origin, Accept, Content-Type, X-Requested-With'



# This method is called when the url of the host is called.
@route('/')
def hello_world():
    ''' Returns a sample text'''
    return "Hello World!"


# hook is a annotation in the Bottle framework. 'after_request' here means, after every request this method should be called. 
@hook('after_request')
def enable_cors():
    ''' Add headers to enable cors'''
    response.headers['Access-Control-Allow-Origin'] = _allow_origin
    response.headers['Access-Control-Allow-Methods'] = _allow_methods
    response.headers['Access-Control-Allow-Headers'] = _allow_headers


# This method will return the crime data in 'JSON' format. This method accepts 'GET' and 'OPTIONS' requests.
@route('/districtcrimetype',method=['OPTIONS','GET'])
def getCrimesByDistrict():
    ''' Add '''
    if request.method == 'OPTIONS': #If a 'OPTIONS' request is made, then empty json is returned and then the enable_cors() method is called, which adds the headers to the response.
        return {}
    
    vcap_services = json.loads(os.environ['VCAP_SERVICES']) #Read the bluemix 'VCAP_SERVICES' environment variables 
    service = vcap_services['dashDB'][0] #Read the dashDb credentials from it.
    credentials = service["credentials"]
    url = 'DATABASE=%s;uid=%s;pwd=%s;hostname=%s;port=%s;' % ( credentials["db"],credentials["username"],credentials["password"],credentials["host"],credentials["port"])
    connection = ibm_db.connect(url,"","") # Connect to dashDB.
    statement = ibm_db.prepare(connection,'SELECT ID,NUM FROM DASH6757.BOSTONCRIME') #Build the query
    ibm_db.execute(statement)  # Run the sql query
    data = ibm_db.fetch_tuple(statement) # Fetch the first row of the result set

    object_store_service = json.loads(os.environ['VCAP_SERVICES'])['Object-Storage'][0] #Read the bluemix 'VCAP_SERVICES' environment variables for Object Data Store
    objectstorage_creds = object_store_service['credentials'] #Get the credentials
    auth_url = objectstorage_creds['auth_url']+'/v3' #Get the auth url and append v3(API version)
    project_id = objectstorage_creds['projectId'] #Get the projectId, uniquer Identifier
    user_id = objectstorage_creds['userId'] #Get the userid 
    region_name = objectstorage_creds['region'] # Get the region where Bluemix Object Data Store servers are hosted. At present they are hosted in two locations. Dallas and London.
    password = objectstorage_creds['password'] # Get the password
    auth_version=3 # Get the version of the api
    # Python SwiftClient api will allow us to authenticate with Object Data Store 
    conn = swiftclient.Connection(key=password,authurl=auth_url,auth_version=auth_version,insecure=True,os_options={"project_id": project_id,"user_id":user_id,"region_name": region_name})
    cont_headers, objs = conn.get_container('notebook') # Object Data Store has the concept of Containers. Containers are a way of dividing files into groups in Object Data Store.
    obj_headers, geo_obj = conn.get_object('notebook','geo.json') #geo.json contains the data in proper json format.
    geo_objJSON = json.loads(geo_obj) #load the json object.
    while(data): # Keep reading the data from the dashDB database until there are no rows left to read.
      if geo_objJSON.has_key(data[0].strip()): # Check if json contains the district that we just read
        crimeDataGeo = geo_objJSON.get(data[0].strip()) # Get the co-ords and district id from the json file for the district id which we just matched.
        crimeDataGeo["COUNT"] = data[1] # add the count of crimes to the json which we fetched in the above line.
        polygon_maps_array = crimeDataGeo["coordinates"] # Read the coordinates
        for idx,val in enumerate(polygon_maps_array): # Convert each geo coordinates into lat lng format which is required by the google maps api
            coordinates = val
            lat_long_json = {}
            if len(coordinates) == 2:
                lat_long_json["lng"] = coordinates[0] # lng
                lat_long_json["lat"] = coordinates[1] # lat
            polygon_maps_array[idx] = lat_long_json #json containing lat and lng data
        crimeDataGeo["coordinates"] = polygon_maps_array # Assign it back to the json.
        geo_objJSON[data[0]] = crimeDataGeo # add the district json back to the geo json.
      data = ibm_db.fetch_tuple(statement) # read the next row from dashDB.
    finalJSON = {} 
    finalJSON['data'] = sorted(geo_objJSON.values(),key=itemgetter('COUNT'), reverse=True) #reads all the values from the geoJSON, sorts them based on COUNT in descending order.
    ibm_db.close(connection) # Close the connection.
    return json.dumps(finalJSON) # return the json as string.



# Reads the Bluemix environment variables and set the PORT, if the environment variable is not set, then it uses 8080
PORT = int(os.getenv('VCAP_APP_PORT','8000'))
#PORT = 8080
# Reads the Bluemix environment variables and set the HOST, if the environment variable is not set, then it uses 0.0.0.0
HOST = str(os.getenv('VCAP_APP_HOST','0.0.0.0'))
#HOST = localhost
# This will start the bottle server. 
run(host=HOST,port=PORT)


