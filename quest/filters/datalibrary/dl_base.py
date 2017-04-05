"""Functions required to assemble and run plugins from the C++ DataLibrary."""
from ..base import FilterBase
from quest import get_pkg_data_path, util
from quest.api import get_metadata, get_datasets, new_dataset, update_metadata, new_feature
from quest.api.projects import active_db
from quest.api.datasets import DatasetStatus

from geojson import Polygon
from jinja2 import Environment, FileSystemLoader
import json
import numpy as np
import os
from subprocess import check_output
import tempfile
from quest.util.log import logger

env = Environment(loader=FileSystemLoader(get_pkg_data_path('datalibrary')))


class DatalibraryBase(FilterBase):
    def register(self, name=None):
        """Register Timeseries

        """
        self.name = name
        self.metadata = {
            'group': 'vitd',
            'operates_on': {
                'datatype': ['vitd'],
                'geotype': None,
                'parameters': None,
            },
            'produces': {
                'datatype': None,
                'geotype': None,
                'parameters': None,
            },
        }

    def _apply_filter(self, datasets, features=None, options=None,
                     display_name=None, description=None, metadata=None):

        datasets = util.listify(datasets)

        # get metadata, path etc from first dataset, i.e. assume all datasets
        # are in same folder. This will break if you try and combine datasets
        # from different services
        dataset = datasets[0]
        orig_metadata = get_metadata(dataset)[dataset]

        if options.get('apply_to_collection'):
            filters = {'collection': orig_metadata['_collection'], 'datatype': 'vitd'}
            datasets = get_datasets(filters=filters)

        bbox = _get_bbox(datasets)
        src = _get_src(dataset)
        orig_metadata = get_metadata(dataset)[dataset]
        if display_name is None:
            display_name = 'Created by filter {}'.format(self.name)
        geom_coords = [util.bbox2poly(*bbox)]
        cname = orig_metadata['_collection']
        feature = new_feature(cname,
                              display_name=display_name, geom_type='Polygon',
                              geom_coords=geom_coords)

        new_dset = new_dataset(feature,
                               source='derived',
                               display_name=display_name,
                               description=description)

        # define dst folder as collection/dataset_name
        prj = os.path.dirname(active_db())
        dst = os.path.join(prj, cname, new_dset)
        util.mkdir_if_doesnt_exist(dst)

        if options is None:
            options = {}

        options.update({'src': src, 'dst': dst, 'bbox': bbox})
        # extra options set by plugins
        options = self._extra_options(options)
        _run_filter(self.template, **options)

        self.file_path = dst
        new_metadata = self._new_dataset_metadata()

        new_metadata.update({
            'options': self.options,
            'status': DatasetStatus.FILTERED,
            'message': 'VITD Filter Applied',
            'file_path': self.file_path,
        })
        update_metadata(new_dset, quest_metadata=new_metadata, metadata=metadata)

        return {'datasets': new_dset, 'features': feature}

    def apply_filter_options(self, fmt):
        raise NotImplementedError

    def _new_dataset_metadata(self):
        raise NotImplementedError

    def _extra_options(self, options):
        """override in inherited classes if more options need to be set"""
        return options


def _render_template(name, **kwargs):
    if 'bbox' in kwargs.keys():
        xmin, ymin, xmax, ymax = kwargs['bbox']
        kwargs['bbox'] = '{} {} {} {}'.format(ymax, xmin, ymin, xmax)

    template = env.get_template(name)
    return template.render(**kwargs)


def _run_filter(name, **kwargs):
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as t:
        t.write(_render_template(name, **kwargs))
        fname = t.name

    runplugin = os.environ.get('QUEST_RUNPLUGIN')
    if runplugin is None:
        raise ValueError('Environment Variable QUEST_RUNPLUGIN not set')
    output = check_output([runplugin, fname])
    logger.info(output)
    return output


def _get_bbox(datasets):

    f = get_metadata(datasets, as_dataframe=True)['_feature'].tolist()
    geom_coords = get_metadata(f, as_dataframe=True)['_geom_coords'].tolist()
    coords = []
    for gc in geom_coords:
        coords.append(np.array(json.loads(gc)))

    coords = np.hstack(coords).squeeze()
    xmax, ymax = coords.max(axis=0)
    xmin, ymin = coords.min(axis=0)
    return [xmin, ymin, xmax, ymax]


def _get_src(dataset):
    file_path = get_metadata(dataset)[dataset].get('file_path')
    file_path = util.listify(file_path)[0]
    # the root vitd directory is three levels above file name
    return os.path.split(os.path.split(os.path.split(file_path)[0])[0])[0]
