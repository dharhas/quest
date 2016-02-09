"""DSL wrapper for USGS NWIS Services

"""
from .base import WebServiceBase
from geojson import Feature, Point, FeatureCollection, dump
import os
import pandas as pd
from .. import util
from .. import isep_mong

# default file path (appended to collection path)
# DEFAULT_FILE_PATH = 'isep/lidar/'
DEFAULT_FILE_PATH = os.path.join('isep','lidar')
CACHE_FILE = 'isep_metadata.json'

class ISEPService(WebServiceBase):
    def _register(self):
        self.metadata = {
            'display_name': 'ISEP',
            'description': 'ISEP Description',
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
                'parameters' : ['none'],
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
                'parameters' : ['none'],
                'unmapped_parameters_available' : True,
                'geom_type' : 'Point',
                'datatype' : 'MET',
                'geographical_areas' : ['United States of America'],
                'bounding_boxes': [(-124.7099609, 24.54233398, -66.98701171, 49.36967773)] 
            },
            'ADH' : {
                'display_name' : 'ADH',
                'description' : 'ADH',
                'service_type' : 'geo-discrete',
                'parameters' : ['none'],
                'unmapped_parameters_available' : True,
                'geom_type' : 'Point',
                'datatype' : 'ADH',
                'geographical_areas' : ['United States of America'],
                'bounding_boxes': [(-124.7099609, 24.54233398, -66.98701171, 49.36967773)] 
            }
        }

    def _get_features(self, service):
        
        driver = isep_mong.MongoDriver()
        sites = []
        bounding_box = [-124.7099609, 24.54233398, -66.98701171, 49.36967773]
        
        sites = driver.get_sites_BoundingBox(bounding_box, service.upper())
        
        features = pd.DataFrame(sites)
        features.index = features['id']
        features['geom_type'] = 'Point'
        features['geom_coords'] = zip(features['longitude'], features['latitude'])
        features['parameters'] = ''
        features['file_format'] = ''
        
        return features
        
        '''
        
        features = pd.DataFrame(sites)
        features.index = features[]


        features.index = features['_id']
        features['geom_type'] = zip(features['longitude'], features['latitude'])
        features['geom_coords'] = features['boxes'].apply(lambda x: [util.bbox2poly(*x[0].split(), reverse_order=True)])
        coords = features['geom_coords'].apply(lambda x: pd.np.array(x).mean(axis=1))
        features['longitude'] = coords.apply(lambda x: x.flatten()[0])
        features['latitude'] = coords.apply(lambda x: x.flatten()[1])
        features['download_url'] = features['links'].apply(lambda x: [link['href'] for link in x if link.get('type')=='application/zip'][0])
        
        
        for site in sites:
            
            files = []
            files = site.get("files", '')
            
            
            for f in files:
                fileproperties = {
                                'file name': f.get("file name", ""),
                                'server': f.get("server", ""),
                                'file location': f.get("file location", ""),
                                'file format': f.get("file format", ""),
                                'cols': f.get("cols", ""),
                                'delimited type': f.get("delimited type", ""),
                                'first line': f.get("first line", ""),
                                'first line def': f.get("first line def", ""),
                                'col num': f.get("col num", ""),
                                'first line def': f.get("first line def", ""),
                                'vertices': f.get("vertices", ""),
                                'X': f.get("X", ""),                                
                                'Y': f.get("Y", ""),
                                'Z': f.get("Z", ""),
                                'location': f.get("location", ""),
                                'header row one': f.get("header row one", ""),
                                'header row two': f.get("header row two", ""),
                                'header row three': f.get("header row three", ""),
                }     
                
                
                siteproperties = {
                                'name': site.get("name", ""),
                                'subsite': site.get("subsite", ""),
                                'data type': site.get("data type", ""),
                                'collection type': site.get("collection type", ""),
                                site.get("data type", "").lower() + ' type': site.get(site.get("data type", "").lower() + " type", ""),
                                'file properties' : fileproperties
                            }
    
                lat = 0
                lon = 0
                
                if "location" in f.keys():
                    lat = f.get("location", "")[0]
                    lon = f.get("location", "")[1]
                    
                featureId =  f.get("file name", "") + ' - ' + site.get("name", "")
                
                feature = Feature(id=featureId,
                                geometry=Point((float(lon),
                                                float(lat))),
                                properties=siteproperties,
                            )
                features.append(feature)

        return FeatureCollection(features)
        '''
        
       

    '''
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
                "data_type": {
                    "type": "string",
                    "description": "a string detailing which data type is searched: e.g. 'lidar'",
                    },
                "all_parameters_required": {
                    "type": "boolean",
                    "description": "If true only locations where all parameters exist will be shown"
                }
            },
            "required": None,
        }
        return schema
    '''

    def _download_dataset_options(self, service):
        pass

    def _download_dataset(self, service, feature, parameter):
        
         '''
         if locations is None:
            raise ValueError("A location needs to be supplied.")
         if not path:
            path = util.get_dsl_dir()
            
         path = os.path.join(path, DEFAULT_FILE_PATH)         
         util.mkdir_if_doesnt_exist(path)
         '''
         
         data_files = {}        
         return data_files
         
    def _get_parameters(self, service, features=None):
        return {
            'parameters': ['none'],
            'parameter_codes': ['none'],
        }

    '''
    def get_data_options(self):
        schema = {
            "title": "ISEP Download Options",
            "type": "object",
            "properties": {
                "locations": {
                    "type": "string",
                    "description": "Optional single or comma delimited list of location identifiers",
                    },
                "path": {
                    "type": "string",
                    "description": "Path where data will be downloaded to.",
                    },
                "location_type": {
                    "type": "string",
                    "description": "a string stating alternate criteria to search: e.g. 'subsite'",
                    },
                "data_type": {
                    "type": "string",
                    "description": "a string detailing which data type is searched: e.g. 'lidar'",
                    },
                "all_parameters_required": {
                    "type": "boolean",
                    "description": "If true only locations where all parameters exist will be shown"
                }
            },
        }
        return schema
    '''
    '''
    def provides(self):
        return ['lidar']
    '''