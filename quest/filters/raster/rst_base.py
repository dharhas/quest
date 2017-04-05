"""Functions required run raster filters"""
from ..base import FilterBase
from quest import util

from quest.api import get_metadata, new_dataset, update_metadata, new_feature, datasets
from quest.api.projects import active_db
from quest.api.datasets import DatasetStatus

import os
import rasterio


class RstBase(FilterBase):
    def register(self, name=None):
        """Register Timeseries

        """
        self.name = name
        self.metadata = {
            'group': 'raster',
            'operates_on': {
                'datatype': ['raster'],
                'geotype': None,
                'parameters': None,
            },
            'produces': {
                'datatype': 'raster',
                'geotype': None,
                'parameters': None,
            },
        }

    def _apply_filter(self, datasets, features=None, options=None,
                     display_name=None, description=None, metadata=None):

        if len(datasets > 1):
            out_image = self._apply(datasets,options)

        else:

            # datasets = util.listify(datasets)
            dataset = datasets

        # get metadata, path etc from first dataset, i.e. assume all datasets
        # are in same folder. This will break if you try and combine datasets
        # from different services

        orig_metadata = get_metadata(dataset)[dataset]
        src_path = orig_metadata['file_path']
        if display_name is None:
            display_name = 'Created by filter {}'.format(self.name)
        if options is None:
            options ={}
        options['orig_meta'] = orig_metadata
        #run filter
        with rasterio.open(src_path) as src:
            out_image = self._apply(src,options)

        out_meta = src.meta.copy()
        # save the resulting raster
        out_meta.update({"driver": "GTiff",
                         "height": out_image.shape[1],
                         "width": out_image.shape[2],
                         "transform": None})

        cname = orig_metadata['_collection']
        feature = new_feature(cname,
                              display_name=display_name, geom_type='Polygon',
                              geom_coords=None)

        new_dset = new_dataset(feature,
                               source='derived',
                               display_name=display_name,
                               description=description)

        prj = os.path.dirname(active_db())
        dst = os.path.join(prj, cname, new_dset)
        util.mkdir_if_doesnt_exist(dst)
        dst = os.path.join(dst, new_dset+'.tif')

        with rasterio.open(dst, "w", **out_meta) as dest:
            dest.write(out_image)

        self.file_path = dst

        new_metadata = {
            'parameter': orig_metadata['parameter'],
            'datatype': orig_metadata['datatype'],
            'file_format': orig_metadata['file_format'],
            'options': self.options,
            'file_path': self.file_path,
            'status': datasets.DatasetStatus.FILTERED,
            'message': 'raster filter applied'
        }
        update_metadata(new_dset, quest_metadata=new_metadata, metadata=metadata)

        return {'datasets': new_dset, 'features': feature}

    def apply_filter_options(self, fmt='json-schema'):
        schema = {}

        return schema
        # if fmt == 'json-schema':
        #     properties = {
        #         "bbox": {
        #             "type": "string",
        #             "description": "bounding box 'xmin, ymin, xmax, ymax'",
        #         },
        #         "nodata": {
        #             "type": "number",
        #             "description": "no data value for raster.",
        #             "default": 0,
        #         },
        #     }
        #
        #     schema = {
        #         "title": "Raster Clip",
        #         "type": "object",
        #         "properties": properties,
        #         "required": ['bbox'],
        #     }
        #
        # if fmt == 'smtk':
        #     schema = ''

    def _apply(self, df, metadata, options):
        raise NotImplementedError

