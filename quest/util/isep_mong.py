#import argparse
import json
#from pymongo import GEO2D
#from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import OperationFailure
import sys
#import os
#import uuid
#from collections import OrderedDict
#import xlrd
#from bson import BSON
#from bson import json_util

class MongoDriver(object):

    def __init__(self):
        """ Attempt to connect and authenticate with the admin database - Later
            this will probably be passed as an argument when people are only
            getting permission for certain databases """

        """ self.host = "134.164.150.36" """
        self.host = "localhost"
        self.port = 27017
        self.dbase = "DATA"
        """ self.username = "guest" """
        """ self.password = "guest" """
        self.db_conn = None
        self.coll_conn = None

        try:
            """ self.client = MongoClient(host=self.host) """
            self.client = MongoClient(self.host, self.port)
        except ConnectionFailure as e:
            sys.stderr.write("Could not connect to MongoDB: %s" % e)
            sys.exit()
        try:
            """ self.client.admin.authenticate(name=self.username, password=self.password) """
            
            self.db_conn = self.client[self.dbase]
            self.connect_to_collection()
            
        except OperationFailure as e:
            sys.stderr.write("Could not authenticate you to admin db: %s" %e)
            sys.exit()

    def connect_to_collection(self, collection=None):
        """ Simply just connects to a collection """

        if collection:
            self.coll_conn = self.db_conn[collection]
        else :
            self.coll_conn = self.db_conn["Sites"]

        return self.coll_conn
        
    def get_ADH_sites(self, bounding_box=None):
        
        self.connect_to_collection("ADH")
        if bounding_box is None:
            bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

        returning = []             
        sites = self.coll_conn.find({"location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}}})
      
        for site in sites:
            
            item = {}
            item["id"] = str(site["_id"])
            item["Site"] = site["name"]
            item["Report Title"] = site["title of report"]
            item["Report Location"] = site["physical location"]
            '''
            send back the first bit of information
            item["Study Purpose"] = site["purpose of study"]
            item["File Server"] = site["file server"]
            item["Base Mesh Location"] = site["base mesh location"]
            item["Plan Mesh Location"] = site["plan mesh location"]
            item["Studies"] = site["studies"]
            item["Data"] = site["data"]
            item["Computation Conditions"] = site["computation conditions"]
            item["Scripts"] = site["scripts"]
            item["Location"] = site["location"]
            '''
            item["latitude"] = site["location"][0]
            item["longitude"] = site["location"][1]
            
            returning.append(item)
        
        return returning
        
    def get_MET_sites(self, bounding_box=None):
        
        self.connect_to_collection()
        if bounding_box is None:
            bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

        returning = []             
        sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}} , "data type":"MET"})

        
        for site in sites:
            for fileItem in site['files']:
                item = {}
                item['id'] = str(site['_id'])
                item['file_format'] = fileItem['file type']
                item['filename'] = fileItem['file name']
                item['download_url'] = fileItem['file location']
                item['site'] = site['name']
                item['subsite'] = site['subsite']
                item['data type'] = site['data type']
                item['collection type'] = site['collection type']
                item['MET type'] = site['met type']
                item['latitude'] = fileItem['location'][0]
                item['longitude'] = fileItem['location'][1]
                item['parameters'] = 'MET'
                item['download_url'] = 'https://localhost/vane/file.txt'
                
                item['display_name'] = item['filename']
                item['description'] = ''
                item['reserved'] = item['download_url']
        
                returning.append()
        
        return returning
        
    def get_LIDAR_sites(self, bounding_box=None):
        
        self.connect_to_collection()
        if bounding_box is None:
            bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

        returning = []             
        sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}} , "data type":"LIDAR"})

        for site in sites:
            for fileItem in site['files']:
                item = {}
                item['id'] = str(site['_id'])
                item['file_format'] = fileItem['file format']
                item['filename'] = fileItem['file name']
                item['download_url'] = fileItem['file location']
                item['site'] = site['name']
                item['subsite'] = site['subsite']
                item['data type'] = site['data type']
                item['collection type'] = site['collection type']
                item['MET type'] = site['lidar type']
                item['latitude'] = fileItem['location'][0]
                item['longitude'] = fileItem['location'][1]
                item['parameters'] = 'LIDAR'
                item['download_url'] = 'https://localhost/vane/file.txt'
                
                item['display_name'] = item['filename']
                item['description'] = ''
                item['reserved'] = item['download_url']
                
                if len(returning) is 0:
                    returning.append(item)
        
        return returning

    def get_sites_BoundingBox(self, bounding_box=None, data_type="LIDAR"):
        """Get Locations associated with service.

        Take a series of query parameters and return a list of 
        locations as a geojson python dictionary
        
        bounding_box : ``None or str,
        comma delimited set of 4 numbers        
        """
        sites = []
        
        if data_type == "LIDAR":
            sites = self.get_LIDAR_sites(bounding_box)
        elif data_type == "MET":
            sites = self.get_MET_sites(bounding_box)
        else :
            sites = self.get_ADH_sites(bounding_box)

        return  sites