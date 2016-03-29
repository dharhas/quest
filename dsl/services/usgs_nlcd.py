"""services based on www.sciencebase.gov."""

import pandas as pd
import requests
from .base import SingleFileBase


class UsgsNlcdService(SingleFileBase):
    def _register(self):
        self.metadata = {
            'display_name': 'National Land Cover Database',
            'description': 'The National Land Cover Database products are created through a cooperative project conducted by the Multi-Resolution Land Characteristics (MRLC) Consortium. The MRLC Consortium is a partnership of federal agencies (www.mrlc.gov), consisting of the U.S. Geological Survey (USGS), the National Oceanic and Atmospheric Administration (NOAA), the U.S. Environmental Protection Agency (EPA), the U.S. Department of Agriculture -Forest Service (USDA-FS), the National Park Service (NPS), the U.S. Fish and Wildlife Service (FWS), the Bureau of Land Management (BLM) and the USDA Natural Resources Conservation Service (NRCS).',
            'organization': {
                'abbr': 'USGS',
                'name': 'United States Geological Survey',
            },
        }

    def _layers(self):
        return {
            '2001': 'NLCD 2001 Land Cover',
            '2006': 'NLCD 2006 Land Cover',
            '2011': 'NLCD 2011 Land Cover',
        }

    def _get_services(self):
        services = {}
        for service, description in self._layers().items():
            services[service] = {
                'display_name': description,
                'description': 'Retrieve NLCD %s' % service,
                'service_type': 'geo-discrete',
                'geographical_areas': ['USA'],
                'parameters': ['landcover'],
                'unmapped_parameters_available': False,
                'bounding_boxes': [[-130.232828, 21.742308, -63.672192, 52.877264]],
                'geom_type': 'polygon',
                'datatype': 'raster',
            }

        return services

    def _get_features(self, service):
        base_url = 'https://www.sciencebase.gov/catalog/items'
        params = [
            ('filter', 'tags!=tree canopy'),
            ('filter', 'tags!=Imperviousness'),
            ('filter', 'tags=GeoTIFF'),
            ('max', 1000),
            ('fields', 'webLinks,spatial,title'),
            ('format', 'json')
        ]

        if service == '2001':
            params.append(('parentId', '4f70a45ee4b058caae3f8db9'))

        if service == '2006':
            params.append(('parentId', '4f70a46ae4b058caae3f8dbb'))

        if service == '2011':
            params.append(('parentId', '513624bae4b03b8ec4025c4d'))

        r = requests.get(base_url, params=params)
        features = pd.DataFrame(r.json()['items'])
        features = features.ix[~features.title.str.contains('Imperv')]
        features = features.ix[~features.title.str.contains('by State')]
        features = features.ix[~features.title.str.contains('Tree Canopy')]
        features['_download_url'] = features.webLinks.apply(_parse_links)
        features['_extract_from_zip'] = '.tif'
        features['_filename'] = features['_download_url'].str.split('FNAME=', expand=True)[1]
        features['_geom_type'] = 'Polygon'
        features['_geom_coords'] = features.spatial.apply(_bbox2poly)
        features['_parameters'] = 'landcover'
        features['_file_format'] = 'raster'
        coords = features['_geom_coords'].apply(lambda x: pd.np.array(x).mean(axis=1))
        features['_longitude'] = coords.apply(lambda x: x.flatten()[0])
        features['_latitude'] = coords.apply(lambda x: x.flatten()[1])
        features.rename(columns={'id': '_service_id', 'title': '_display_name'},
                  inplace=True)
        features.index = features['_service_id']

        # remove extra fields. nested dicts can cause problems
        del features['relatedItems']
        del features['webLinks']
        del features['spatial']
        del features['link']

        return features

    def _get_parameters(self, service, features=None):
        return {
            '_parameters': ['landcover'],
            '_parameter_codes': ['landcover'],
        }


def _bbox2poly(bbox):
    xmin = bbox['boundingBox']['minX']
    xmax = bbox['boundingBox']['maxX']
    ymin = bbox['boundingBox']['minY']
    ymax = bbox['boundingBox']['maxY']

    return [[
        [xmin, ymin],
        [xmin, ymax],
        [xmax, ymax],
        [xmax, ymin],
        [xmin, ymin]
        ]]


def _parse_links(links):
    return [link['uri'] for link in links if link['type'] == 'download'][0]
