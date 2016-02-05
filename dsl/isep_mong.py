#import argparse
#import json
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

       
        self.host = "134.164.150.36"
        self.dbase = "Data"
        self.username = "guest"
        self.password = "guest"
        self.db_conn = None
        self.coll_conn = None

        try:
            self.client = MongoClient(host=self.host)
        except ConnectionFailure, e:
            sys.stderr.write("Could not connect to MongoDB: %s" % e)
            sys.exit()
        try:
            self.client.admin.authenticate(name=self.username, password=self.password)
            self.db_conn = self.client[self.dbase]
            self.connect_to_collection()
            
        except OperationFailure, e:
            sys.stderr.write("Could not authenticate you to admin db: %s" %e)
            sys.exit()

    def connect_to_collection(self, collection=None):
        """ Simply just connects to a collection """

        if collection:
            self.coll_conn = self.db_conn[collection]
        else :
            self.coll_conn = self.db_conn["Sites"]

        return self.coll_conn
        
        
    def get_MET_sites(self, bounding_box=None):
        
        self.connect_to_collection()
        if bounding_box is None:
            bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

        returning = []             
        sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}} , "data type":"MET"})

        
        for site in sites:
            for fileItem in site["files"]:
                item = {}
                item["id"] = str(site["_id"])
                item["file name"] = fileItem["file name"]
                item["data type"] = site["data type"]
                item["collection type"] = site["collection type"]
                item["MET type"] = site["met type"]
                item["site"] = site["name"]
                item["subsite"] = site["subsite"]
                item["latitude"] = fileItem["location"][0]
                item["longitude"] = fileItem["location"][1]
            
                returning.append(item)
        
        return returning
        
    def get_LIDAR_sites(self, bounding_box=None):
        
        self.connect_to_collection()
        if bounding_box is None:
            bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

        returning = []             
        sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}} , "data type":"LIDAR"})

        for site in sites:
            for fileItem in site["files"]:
                item = {}
                item["id"] = str(site["_id"])
                item["file name"] = fileItem["file name"]
                item["data type"] = site["data type"]
                item["collection type"] = site["collection type"]
                item["lidar type"] = site["lidar type"]
                item["site"] = site["name"]
                item["subsite"] = site["subsite"]
                item["latitude"] = fileItem["location"][0]
                item["longitude"] = fileItem["location"][1]
            
                returning.append(item)
        
        return returning

    def get_sites_BoundingBox(self, bounding_box=None, data_type="LIDAR"):
        """Get Locations associated with service.

        Take a series of query parameters and return a list of 
        locations as a geojson python dictionary
        
        bounding_box : ``None or str,
        comma delimited set of 4 numbers        
        """
        
        if data_type == "LIDAR":
            return self.get_LIDAR_sites(bounding_box)
        elif data_type == "MET":
            return self.get_MET_sites(bounding_box)
        else :
            return {}
    
    
