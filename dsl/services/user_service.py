"""Plugin to create service from data in local/network folder

"""

from .. import util
from .base import WebServiceBase

from fs.opener import fsopen, opener
from fs.utils import copyfile
import geojson
from geojson import Feature, FeatureCollection, Polygon
import pandas as pd
import os
import yaml


class UserService(WebServiceBase):
    def __init__(self, uri, name=None, use_cache=True, update_frequency='M'):
        self.name = name
        self.uri = uri
        self.use_cache = use_cache  # not implemented
        self.update_frequency = update_frequency  # not implemented
        self._register(uri)

    def _register(self, uri):
        """Register user service."""

        config_file = self._get_path('dsl.yml')
        c = yaml.load(fsopen(config_file, 'r'))
        self.metadata = c['metadata']
        self.metadata['service_uri'] = uri
        self.services = c['services']
        self.name = c['name']

    def _get_services(self):
        return {k: v['metadata'] for k, v in self.services.items()}

    def _get_features(self, service):
        fmt = self.services[service]['features']['format']
        features_file = self.services[service]['features']['file']
        path = self._get_path(features_file, service)

        all_features = []
        for p in util.listify(path):
            with fsopen(p) as f:
                if fmt.lower() == 'geojson':
                    features = geojson.load(f)
                    features = util.to_dataframe(features)

                if fmt.lower() == 'mbr':
                    # TODO creating FeatureCollection not needed anymore
                    # this can be rewritten as directly creating a pandas dataframe
                    polys = []
                    # skip first line which is a bunding polygon
                    f.readline()
                    for line in f:
                        feature_id, x1, y1, x2, y2 = line.split()
                        properties = {'feature_id': feature_id}
                        polys.append(Feature(geometry=Polygon([util.bbox2poly(x1, y1, x2, y2)]), properties=properties, id=feature_id))
                    features = FeatureCollection(polys)
                    features = util.to_dataframe(features)

                if fmt.lower() == 'mbr-csv':
                    # TODO merge this with the above,
                    # mbr format from datalibrary not exactly the same as
                    # mbr fromat in dsl-demo-data
                    polys = []
                    for line in f:
                        feature_id, y1, x1, y2, x2 = line.split(',')
                        feature_id = feature_id.split('.')[0]
                        properties = {'feature_id': feature_id}
                        polys.append(Feature(geometry=Polygon([util.bbox2poly(x1, y1, x2, y2)]), properties=properties, id=feature_id))
                    features = FeatureCollection(polys)
                    features = util.to_dataframe(features)

            all_features.append(features)

        # drop duplicates fails when some columns have nested list/tuples like
        # _geom_coords. so drop based on index
        features = pd.concat(all_features)
        features['index'] = features.index
        features = features.drop_duplicates(subset='index')
        features = features.set_index('index').sort_index()
        return features

    def _get_parameters(self, service, features=None):
        metadata = self.services[service]['metadata']
        params = metadata['parameters']
        param_codes = metadata.get('parameter_codes', params)
        return {
            '_parameters': params,
            '_parameter_codes': param_codes,
        }

    def _download(self, service, feature, save_path, dataset, parameter=None, **kwargs):
        datasets = self.services[service]['datasets']
        save_folder = datasets.get('save_folder')
        mapping = datasets['mapping']
        fname = mapping.replace('<feature>', feature)
        final_path = []
        if parameter is not None:
            fname = fname.replace('<parameter>', parameter)
        path = self._get_path(fname, service)
        service_folder = self.services[service].get('service_folder', [''])
        for src, svc_folder in zip(util.listify(path), util.listify(service_folder)):
            src_fs, src_file = opener.parse(src)
            dst = save_path
            if save_folder is not None:
                dst = os.path.join(dst, save_folder, svc_folder)

            util.mkdir_if_doesnt_exist(dst)
            dst = os.path.join(dst, fname)
            final_path.append(dst)
            dst_fs, dst_file = opener.parse(dst)
            copyfile(src_fs, src_file, dst_fs, dst_file)

        # TODO copy common files
        # May not be needed anymore
        # print('value =', kwargs.get('copy_common'))
        # print('src=', src_path)
        # if kwargs.get('copy_common'):
        #    common_files = [f for f in os.listdir(src_path) if os.path.isfile(os.path.join(src_path,f))]
        #     print('files=', common_files)
        #    for f in common_files:
        #        print('copying common file: ', f)
        #        shutil.copy(os.path.join(src_path,f), path)

        # TODO need to deal with parameters if multiple params exist
        if len(final_path) == 1:
            final_path = final_path[0]
        else:
            final_path = ','.join(final_path)

        metadata = {
            'save_path': final_path,
            'file_format': self.services[service]['metadata'].get('file_format'),
            'datatype': self.services[service]['metadata'].get('datatype'),
            'parameter': self.services[service]['metadata'].get('parameters')[0],
        }
        extra_meta = datasets.get('metadata')
        if extra_meta is not None:
            metadata.update(extra_meta)
        return metadata

    def _download_options(self, service):
        # TODO if more than one parameter available
        return {}

    def _get_path(self, filename, service=None):
        uri = self.uri
        if service is None:
            if uri.startswith('http'):
                path = uri.rstrip('/') + '/' + filename
            else:
                path = os.path.join(uri, filename)
        else:
            folder = self.services[service].get('service_folder')
            if folder is None:
                return self._get_path(filename)
            else:
                if not isinstance(folder, list):
                    folder = [folder]
                path = []
                for f in folder:
                    if uri.startswith('http'):
                        path.append(uri.rstrip('/') + '/{}/{}'.format(f, filename))
                    else:
                        path.append(os.path.join(uri, f, filename))
        if isinstance(path, list) and len(path) == 1:
            path = path[0]
        return path
