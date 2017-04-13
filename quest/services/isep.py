from .base import SingleFileBase
from .. import util
from ..util import isep_mong
from geojson import FeatureCollection, dump
import json
import pandas as pd
import geopandas as gpd
import os

BASE_PATH = 'isep'
DEFAULT_FILE_PATH = 'isep'
CACHE_FILE = 'isep_%s_metadata.json'

class ISEPService(SingleFileBase):
    def _register(self):
        self.metadata = {
            'display_name': 'ISEP Service',
            'description': 'Services available for ISEP Data',
            'organization': {
                'abbr': 'ISEP',
                'name': 'Integrated Simulation Environment Phenomenology',
            },
        }

    def _get_services(self):
        return {
            'LIDAR' : {
                'display_name' : 'LIDAR',
                'description' : 'A detection system that works on the principle of radar, but uses light from a laser',
                'service_type' : 'geo-discrete',
                'parameters' : ["LIDAR"],
                'unmapped_parameters_available' : True,
                'geom_type' : 'Point',
                'datatype' : 'LIDAR',
                'geographical_areas' : ['United States of America'],
                'bounding_boxes': [(-124.7099609, 24.54233398, -66.98701171, 49.36967773)]   
            },
            'MET' : {
                'display_name' : 'MET',
                'description' : 'MET',
                'service_type' : 'geo-discrete',
                'parameters' : ["MET"],
                'unmapped_parameters_available' : True,
                'geom_type' : 'Point',
                'datatype' : 'MET',
                'geographical_areas' : ['United States of America'],
                'bounding_boxes': [(-124.7099609, 24.54233398, -66.98701171, 49.36967773)] 
            },
        }

    def _get_features(self, service):
        driver = isep_mong.MongoDriver()
        bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773] 
        features = driver.get_sites_BoundingBox(bounding_box, service.upper())
                
        print("Type of features: ") 
        print(type(features))
        print("Type of features[0]: ");
        print(type(features[0]))
        print("Features:")
        print(features)
        
        dfeatures = pd.DataFrame(features)
        """features = gpd.GeoDataFrame.from_dict(features, orient='index')
        
        return dfeatures

    def _get_parameters(self, service, features=None):
        if service=='LIDAR':
            
            parameters = {
                'parameters': 'LIDAR',
                'parameter_codes': 'LIDAR'
            }

        if service=='MET':
            
            parameters = {
                'parameters': 'MET',
                'parameter_codes': 'MET'
            }
            
        if service=='ADH':
            
            parameters = {
                'parameters': 'ADH',
                'parameter_codes': 'ADH'
            }

        return parameters

    