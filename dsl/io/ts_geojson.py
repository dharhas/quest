"""I/O plugin for Geojson with timeseries


"""
from .base import IoBase
from .. import util
from geojson import Feature, Point, dump
import json
import os
import pandas as pd


class TsGeojson(IoBase):
    """Base class for I/O for different file formats
    """

    def register(self):
        """Register plugin by setting description and io type 
        """
        self.description = 'Reader/Writer for Geojson with timeseries in properties field'
        self.iotype = 'timeseries' 

    def read(self, path, as_dataframe=True):
        """Read data from format
        """
        with open(path) as f:
            data = json.load(f)

        if as_dataframe:
            properties = data['properties']
            metadata = properties.pop('metadata', None)
            time = properties.pop('time')
            data = pd.DataFrame(data=properties, index=pd.to_datetime(time))

        return data

    def write(self, path, location_id, geometry, dataframe, metadata=None):
        """Write data to format
        """

        properties={
            'time': dataframe.index.to_native_types(),
            'metadata': metadata,
        }

        for parameter in dataframe.columns:
            properties.update({parameter: dataframe[parameter].tolist()})

        feature = Feature(id=location_id,
                        geometry=geometry,
                        properties=properties,
                    )
        if not path.endswith('.json'):
            path += '.json'

        base, fname = os.path.split(path)
        util.mkdir_if_doesnt_exist(base)

        with open(path, 'w') as f:
            dump(feature, f)

        print 'file written to: %s' % path
