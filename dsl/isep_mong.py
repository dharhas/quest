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

    def get_sites_location(self, locations=None, location_type="name", data_type="LIDAR"):
        """Get Locations associated with service.

        Take a series of query parameters and return a list of 
        locations as a geojson python dictionary
        
        locations : ``None`` or str,
        comma separated list of location codes to fetch
        """

        sites = []
        
        if locations:

            name = location_type
            
            sites = self.coll_conn.find({name:{ "$in" : locations}, "data type":data_type})
        else:
            sites = self.coll_conn.find()            
        
        return sites
            
    def get_sites_BoundingBox(self, bounding_box=None, data_type="LIDAR"):
        """Get Locations associated with service.

        Take a series of query parameters and return a list of 
        locations as a geojson python dictionary
        
        bounding_box : ``None or str,
        comma delimited set of 4 numbers        
        """

        sites = []
        
        if bounding_box:
            sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}} , "data type":data_type})
            '''sites = self.coll_conn.find({"files.location": {"$within": {"$box": [[bounding_box[1], bounding_box[0]], [bounding_box[3], bounding_box[2]]]}}})'''
        else:
            sites = self.coll_conn.find()            
        
        return sites

