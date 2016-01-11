"""DSL wrapper for USGS NWIS Services

"""
from .base import WebServiceBase
import concurrent.futures
from functools import partial
import pandas as pd
import os
from ulmo.usgs import nwis
from .. import util


class NwisService(WebServiceBase):
    def _register(self):
        self.metadata = {
            'display_name': 'USGS NWIS Web Services',
            'description': 'Services available through the USGS National Water Information System',
            'organization': {
                'abbr': 'USGS',
                'name': 'United States Geological Survey', 
            },
        }

    def _get_services(self):
        return {
            'iv': {
                'display_name': 'NWIS Instantaneous Values Service',
                'description': 'Retrieve current streamflow and other real-time data for USGS water sites since October 1, 2007',
                'service_type': 'geo-discrete',
                'parameters': ['streamflow', 'gage_height', 'water_temperature'],
                'unmapped_parameters_available': True,
                'geom_type': 'Point',
                'datatype': 'timeseries',
                'geographical_areas': ['Alaska', 'USA', 'Hawaii'],
                'bounding_boxes' : [
                    (-178.19453125, 51.6036621094, -130.0140625, 71.4076660156),
                    (-124.709960938, 24.5423339844, -66.9870117187, 49.3696777344),
                    (-160.243457031, 18.9639160156, -154.804199219, 22.2231445312),
                ],
            },
            'dv': {
                'display_name': 'NWIS Daily Values Service',
                'description': 'Retrieve historical summarized daily data about streams, lakes and wells. Daily data available for USGS water sites include mean, median, maximum, minimum, and/or other derived values.',
                'service_type': 'geo-discrete',
                'parameters': ['streamflow', 'gage_height', 'water_temperature'],
                'unmapped_parameters_available': True,
                'geom_type': 'Point',
                'datatype': 'timeseries',
                'geographical_areas': ['Alaska', 'USA', 'Hawaii'],
                'bounding_boxes' : [
                    (-178.19453125, 51.6036621094, -130.0140625, 71.4076660156),
                    (-124.709960938, 24.5423339844, -66.9870117187, 49.3696777344),
                    (-160.243457031, 18.9639160156, -154.804199219, 22.2231445312),
                ],
            }
        }

    def _get_features(self, service):
        func = partial(_nwis_features, service=service)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            sites = executor.map(func, _states())
                
        sites = {k: v for d in sites for k, v in d.items()}
        df = pd.DataFrame.from_dict(sites, orient='index')
        df['geom_type'] = 'Point'
        for col in ['latitude', 'longitude']:
            df[col] = df['location'].apply(lambda x: float(x[col]))

        df['geom_coords'] = zip(df['longitude'], df['latitude'])
        return df

    def _get_parameters(self, service, features=None):
        if features is None:
            df = self.get_features(service)
        else:
            df = features

        chunks = list(_chunks(df.index.tolist()))
        func = partial(_site_info, service=service)
        with concurrent.futures.ProcessPoolExecutor() as executor:
            data = executor.map(func, chunks)
        
        data = pd.concat(data, ignore_index=True)
        data.rename(columns={'parm_cd': 'parameter_code', 'site_no': 'feature_id', 'count_nu': 'count'}, inplace=True)
        data = data[pd.notnull(data['parameter_code'])]
        data['parameter'] = data['parameter_code'].apply(lambda x: self._parameter_map(service).get(x))
        data = data[['parameter', 'parameter_code', 'feature_id', 'begin_date', 'end_date', 'count']]

        pm_codes = _pm_codes()
        data['description'] = data['parameter_code'].apply(lambda x: pm_codes.ix[x]['SRSName'] if x in pm_codes.index else '')
        data['unit'] = data['parameter_code'].apply(lambda x: pm_codes.ix[x]['parm_unit'] if x in pm_codes.index else '')
        return data


    def _parameter_map(self, service):
        return {
            '00060': 'streamflow',
            '00065': 'gage_height',
            '00010': 'water_temperature',
        }


    def _download_dataset(self, feature, parameter, path, start=None, end=None, period=None):
        
        if not any([start, end, period]):
            period = 'P365D' #default to past 1yr of data

        if path is None:
            path = util.get_dsl_dir()

        path = os.path.join(path, DEFAULT_FILE_PATH)
        io = util.load_drivers('io', 'ts-geojson')['ts-geojson'].driver

        parameter_codes = []
        statistic_codes = []
        for parameter in parameters:
            p, s = _as_nwis(parameter)
            parameter_codes.append(p)
            statistic_codes.append(s)

        parameter_codes = ','.join(set(parameter_codes))
        statistic_codes = [_f for _f in set(statistic_codes) if _f]
        if statistic_codes:
            statistic_codes = ','.join(statistic_codes)
        else:
            statistic_codes=None

        data_files = {}
        for location in locations:
            data_files[location] = {}
            datasets = nwis.get_site_data(location, parameter_code=parameter_codes,
                                        statistic_code=statistic_codes,
                                        start=start, end=end, period=period,
                                        service=self.service)

            for code, data in datasets.items():
                df = pd.DataFrame(data['values'])
                if df.empty:
                    print('No data found, try different time period')
                    continue
                    
                df.index = self._make_index(df)
                p, s = _as_nwis(code, invert=True)
                if s:
                    parameter = ':'.join([p,s])
                else:
                    parameter = p

                df = df[['value']]
                df.value = df.value.apply(np.float)
                df.columns = [parameter + '(%s)' % data['variable']['units']['code']]
                filename = path + 'nwis_%s_stn_%s_%s.json' % (self.service, location, parameter)
                data_files[location][parameter] = filename
                location_id = data['site']['code']
                geometry = Point((float(data['site']['location']['longitude']), float(data['site']['location']['latitude'])))
                metadata = data['site']
                io.write(filename, location_id=location_id, geometry=geometry, dataframe=df, metadata=metadata)

        return data_files


    def _download_dataset_options(self, service):
        pass


def _chunks(l, n=100):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def _nwis_features(state, service):
    return nwis.get_sites(state_code=state, service=service)


def _nwis_parameters(site, service):
    return {site: nwis.get_site_data(site, service=service).keys()}


def _states():
    return [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ]


def _parse_rdb(url, index=None):
    df = pd.read_table(url, comment='#')
    if index is not None:
        df.index = df[index]

    df = df.ix[1:] #remove extra header line
    return df


def _pm_codes(update_cache=False):
    url = 'http://help.waterdata.usgs.gov/code/parameter_cd_query?fmt=rdb&group_cd=%'
    cache_file = os.path.join(util.get_cache_dir(), 'usgs_pmcodes.h5')
    if update_cache:
        pm_codes = _parse_rdb(url, index='parm_cd')
    else:     
        try:
            pm_codes = pd.read_hdf(cache_file, 'table')
        except:
            pm_codes = _parse_rdb(url, index='parm_cd')
            pm_codes.to_hdf(cache_file, 'table')
    
    return pm_codes


def _stat_codes():
    url = 'http://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%25&fmt=rdb'
    return _parse_rdb(url, index='stat_CD')


def _site_info(sites, service):
    base_url = 'http://waterservices.usgs.gov/nwis/site/?format=rdb,1.0&sites=%s'
    url = base_url % ','.join(sites) + '&seriesCatalogOutput=true&outputDataTypeCd=%s&hasDataTypeCd=%s' % (service, service)
    return _parse_rdb(url)


def _as_nwis(parameter, invert=False):
    
    if ':' in parameter:
        p, s = parameter.split(':')
    else:
        p, s = parameter, None

    codes = {
            'streamflow': '00060',
            'gageheight': '00065',
        }

    stats = {
        'dailymax': '00001',
        'dailymin': '00002',
        'dailymean': '00003',
        None: None,
    }

    if invert:
        codes = {v: k for k, v in list(codes.items())}
        stats = {v: k for k, v in list(stats.items())}
        stats['00011'] = None

    return codes[p], stats[s]
