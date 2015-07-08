"""DSL wrapper for USGS NWIS Services

"""
from .base import DataServiceBase
from geojson import Feature, Point, FeatureCollection
import numpy as np
import pandas as pd
import re
import os
from .. import util
from .. import isep_mong

# default file path (appended to collection path)
DEFAULT_FILE_PATH = 'isep/'

class ISEPBase(DataServiceBase):
    def register(self):
        self.metadata = {
                    'provider': {
                        'abbr': 'ISEP',
                        'name': 'Integrated Simulation Environment Phenomenology',
                    },
                    'display_name': '',
                    'service': '',
                    'description': '',
                    'geographical area': 'United States of America',
                    'bounding_boxes': [[-124.7099609, 24.54233398, -66.98701171, 49.36967773]],
                    'geotype': 'points',
                    'datatype': ''
                }

    def get_locations(self, locations=None, bounding_box=None, location_type="name"):

        driver = isep_mong.MongoDriver()
        sites = []
        
        if locations:
            sites = driver.get_sites_location(locations, location_type)
        else:
            if bounding_box is None:
                bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]

            sites = driver.get_sites_BoundingBox(bounding_box)
            
                    
        features = []
        for site in sites:
            
            files = []
            files = site["files"]
            
            for f in files:
                fileproperties = {
                                'file name': f['file name'],
                                'server': f['server'],
                                'file location': f['file location'],
                                'file format': f['file format'],
                                'cols': f['cols'],
                                'delimited type': f['delimited type'],
                                'first line': f['first line'],
                                'first line def': f['first line def'],
                                'col num': f['col num'],
                                'first line def': f['first line def'],
                                'vertices': f['vertices'],
                                'X': f['X'],                                
                                'Y': f['Y'],
                                'Z': f['Z'],
                                'location': f['location'],
                                'header row one': f['header row one'],
                                'header row two': f['header row two'],
                                'header row three': f['header row three']
                }     
                
                
                siteproperties = {
                                'name': site['name'],
                                'subsite': site['subsite'],
                                'data type': site['data type'],
                                'collection type': site['collection type'],
                                site['data type'].lower() + ' type': site[site['data type'].lower() + ' type'],
                                'file properties' : fileproperties
                            }
    
                feature = Feature(id=site['_id'],
                                geometry=Point((float(f['location'][1]),
                                                float(f['location'][0]))),
                                properties=siteproperties,
                            )
                features.append(feature)

        return FeatureCollection(features)

    def get_locations_options(self): 
        schema = {
            "title": "Location Filters",
            "type": "object",
            "properties": {
                "locations": {
                    "type": "string",
                    "description": "Optional single or comma delimited list of location identifiers",
                    },
                "bounding_box": {
                    "type": "string",
                    "description": "bounding box should be a comma delimited set of 4 numbers ",
                    },
                "location_type": {
                    "type": "string",
                    "description": "a string stating alternate criteria to search: e.g. 'subsite'",
                    },
                "all_parameters_required": {
                    "type": "boolean",
                    "description": "If true only locations where all parameters exist will be shown"
                }
            },
            "required": None,
        }
        return schema

    def get_data_options(self, **kwargs):
        schema = {
            "title": "ISEP Download Options",
            "type": "object",
            "properties": {
                "locations": {
                    "type": "string",
                    "description": "Optional single or comma delimited list of location identifiers",
                    }
            },
        }
        return schema

    def get_data(self, locations, parameters=None):
        
        data_files = {}
        return data_files

