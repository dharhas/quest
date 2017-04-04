from ..base import FilterBase
from quest import util
from quest.api import get_metadata, new_dataset, update_metadata, new_feature
from quest.api.projects import active_db
import terrapin
import os
import rasterio
from pyproj import Proj
import subprocess
# from ....util import logger

class RstMerge(FilterBase):
    def register(self, name='raster-merge'):
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


        if len(datasets) < 2:
            raise ValueError('There must be at LEAST two datasets for this filter')

        orig_metadata = get_metadata(datasets[0])[datasets[0]]
        raster_files = [get_metadata(dataset)[dataset]['file_path'] for dataset in datasets]

        for dataset in datasets:
            if get_metadata(dataset)[dataset]['parameter'] != orig_metadata['parameter']:
                raise ValueError('Parameters must match for all datasets')
            if get_metadata(dataset)[dataset]['unit'] != orig_metadata['unit']:
                raise ValueError('Units must match for all datasets')

        if display_name is None:
            display_name = 'Created by filter {}'.format(self.name)

        if options is None:
            options = {}

        cname = orig_metadata['collection']
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
        dst = os.path.join(dst, new_dset + '.tif')

        self.file_path = dst

        base_path = os.environ["PYTHONPATH"].split(os.pathsep)
        base_path = base_path[0]
        # logger.info('Mosaic and clip to bounding box extents')
        output_vrt = os.path.splitext(dst)[0] + '.vrt'
        gdal_build_vrt = os.path.join(base_path, 'gdalbuildvrt')
        gdal_warp = os.path.join(base_path, 'gdalwarp')

        subprocess.check_output([gdal_build_vrt, '-overwrite', output_vrt] + raster_files)

        subprocess.check_output(
            [gdal_warp, '-overwrite', output_vrt, dst])
        # logger.info('Output raster saved at %s', output_path)


        new_metadata = {
            'parameter': orig_metadata['parameter'],
            'datatype': orig_metadata['datatype'],
            'file_format': orig_metadata['file_format'],
            'unit': orig_metadata['unit']
        }

        if description is None:
            description = 'Raster Filter Applied'

        # update metadata
        new_metadata.update({
            'options': self.options,
            'file_path': self.file_path,
        })
        update_metadata(new_dset, quest_metadata=new_metadata, metadata=metadata)

        return {'datasets': new_dset, 'features': feature}

    def apply_filter_options(self, fmt, **kwargs):
        raise NotImplementedError
