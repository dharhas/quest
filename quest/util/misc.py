import appdirs
from geojson import LineString, Point, Polygon, Feature, FeatureCollection, MultiPolygon
import shapely.geometry
from jinja2 import Environment, FileSystemLoader
import itertools
from .config import get_settings
from .. import get_pkg_data_path
import os
import pandas as pd
import geopandas as gpd
import re
from ..util.log import logger

from jsonrpc.jsonrpc import JSONRPCRequest
from stevedore import extension, driver
try:
    import simplejson as json
except ImportError:
    import json

from uuid import uuid4, UUID


def append_features(old, new):
    if not old:
        return new

    existing_features = [feature['id'] for feature in old['features']]
    for feature in new['features']:
        if feature['id'] not in existing_features:
            old['features'].append(feature)

    return old


def bbox2poly(x1, y1, x2, y2, reverse_order=False, as_geojson=False, as_shapely=False):
    if reverse_order:
        x1, y1 = y1, x1
        x2, y2 = y2, x2

    xmin, xmax = [float(x1), float(x2)]
    ymin, ymax = [float(y1), float(y2)]

    poly = list([[xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin]])
    poly.append(poly[0])

    if not (as_geojson or as_shapely):
        return poly

    if as_geojson:
        polygon = Polygon
        multi_polygon = MultiPolygon

    if as_shapely:
        polygon = shapely.geometry.Polygon
        multi_polygon = shapely.geometry.MultiPolygon

    xmin2 = xmax2 = None
    if xmin < -180:
        xmin2 = 360 + xmin
        xmin = -180
    if xmax > 180:
        xmax2 = xmax - 360
        xmax = 180

    if xmin2 is None and xmax2 is None:
        return polygon(poly)
    # else bbox spans 180 longitude so create multipolygon

    poly1 = list([[xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin]])
    poly1.append(poly1[0])

    xmin = xmin2 or -180
    xmax = xmax2 or 180

    poly2 = list([xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin])
    poly2.append(poly2[0])

    return multi_polygon(polygons=[polygon(poly1), polygon(poly2)])


def build_smtk(smtk_subdir, smtk_filename, **kwargs):
    """build smtk file from template and kwargs and stream as string.

    """
    env = Environment(loader=FileSystemLoader(os.path.join(get_pkg_data_path('smtk'), smtk_subdir)))
    return env.get_template(smtk_filename).render(**kwargs)


def classify_uris(uris, grouped=True, as_dataframe=True, require_same_type=False, exclude=None):
    """convert list of uris into a pandas dataframe.

    classified by resource type
    """
    uris = listify(uris)

    df = pd.DataFrame(uris, columns=['uri'])
    df['type'] = 'collections'

    uuid_idx = df['uri'].apply(is_uuid)
    service_idx = df['uri'].str.startswith('svc://')
    feature_idx = uuid_idx & df['uri'].str.startswith('f')
    dataset_idx = uuid_idx & df['uri'].str.startswith('d')

    df['type'][service_idx] = 'services'
    df['type'][feature_idx] = 'features'
    df['type'][dataset_idx] = 'datasets'
    df.set_index('uri', drop=False, inplace=True)

    grouped_df = df.groupby('type')

    if exclude is not None:
        for uri_type in exclude:
            if uri_type in grouped_df.groups:
                raise ValueError('Uris for {0} are not allowed.'.format(uri_type))

    if require_same_type and len(grouped_df.groups.keys()) > 1:
        raise ValueError('All uris must be of the same type')

    if not as_dataframe:
        groups = {k: list(v) for k, v in grouped_df.groups.items()}
        return groups

    if grouped:
        return grouped_df

    return df


def get_cache_dir(service=None):
    settings = get_settings()
    path = _abs_path(settings['CACHE_DIR'])
    if service is not None:
        path = os.path.join(path, service)

    return path


def get_quest_dir():
    settings = get_settings()
    return settings['BASE_DIR']


def mkdir_if_doesnt_exist(dir_path):
    """makes a directory if it doesn't exist"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def get_projects_dir():
    settings = get_settings()
    return _abs_path(settings['PROJECTS_DIR'], mkdir=False)


def is_remote_uri(path):
    """check if uri points to a http/https url."""
    return bool(re.search('^https?\://', path))


def parse_uri(uri):
    """parse uri and return dictionary.

    if uri starts with svc then parse as a service
    otherwise it is a collection name
    if uri is a uuid then it is either a feature or a dataset

    service uri definition
        <service://<provider[:service]>/<feature or query]>
        <collection>/

    args:
        uri (string): uri string to parse

    return:
        parsed_uri (tuple): resource,

    examples:
        svc://usgs-nwis:iv/0803200
        service://gebco-bathymetry

        project://myproj:mycol/93d2e03543224096b14ce2eacd2eb275/temperature/472e7a7dd177405192fcb47a0c959c9d
        project://myproj:mycol/52b588510ce948b2a2515da02024c53e/temperature/

        service://<name>:<datalayer>/<feature>
        project://<name>:<collection>/<feature>/<dataset>
        feature://<collection>/<feature>/<dataset>
    """

    if uri.startswith('svc://'):
        resource, name, feature = uri.split('://')[-1].split('/')

    uri_dict = {}
    uri_dict['resource'], remainder = uri.split('://')
    parts = remainder.split('/')

    if uri_dict['resource'] not in ['collection', 'service']:
        raise ValueError('Unknown resource type in uri: %s' % uri_dict['resource'])

    if uri_dict['resource'] == 'service':
        keys = ['name', 'feature']

    if uri_dict['resource'] == 'collection':
        keys = ['name', 'feature', 'dataset']

    uri_dict.update({k: parts[i].strip() if i < len(parts) else None for i, k in enumerate(keys)})
    return uri_dict


def parse_service_uri(uri):
    """parse service uri into provider, service, feature.

    Examples:
        usgs-nwis:dv/0800345522
        gebco-bathymetry
        usgs-ned:1-arc-second

    args:
            uri (string): service uri_dict

        returns:
            parsed_uri (tuple): tuple containing provider, service, feature
    """
    svc, feature = (uri.split('://')[-1].split('/') + [None])[:2]
    provider, service = (svc.split(':') + [None])[:2]

    return provider, service, feature


def rpc2py(jsonrpc_str):
    """Convert jsonrpc dict or string to a python quest function call

    args:
        jsonrpc_str (string): json rpc request

    returns:
        converts to python and calls quest function
    """
    import quest
    r = JSONRPCRequest.from_json(jsonrpc_str).data
    fn = getattr(quest.api, r.get('method'))
    kwargs = r.get('params')
    return fn(**kwargs)


def listify(liststr, delimiter=','):
    """If input is a string, split on comma and convert to python list
    """
    if liststr is None:
        return None

    if isinstance(liststr, dict):
        return liststr

    return liststr if isinstance(liststr, list) else [s.strip() for s in liststr.split(delimiter)]


def list_drivers(namespace):
    namespace = 'quest.' + namespace
    mgr = extension.ExtensionManager(
            namespace=namespace,
            invoke_on_load=False,
        )
    return [x.name for x in mgr]


def load_drivers(namespace, names=None):
    names = listify(names)
    namespace = 'quest.' + namespace

    if names is None:
        mgr = extension.ExtensionManager(
            namespace=namespace,
            invoke_on_load=True,
        )
        return dict((x.name, x.obj) for x in mgr)

    return {name: driver.DriverManager(namespace, name, invoke_on_load='True') for name in names}


def load_service(uri):
    if not isinstance(uri, dict):
        uri = parse_uri(uri)
    return load_drivers('services', names=uri['name'])[uri['name']].driver


def load_services():
    settings = get_settings()

    # add web services
    web_services = list_drivers('services')
    web_services.remove('user')

    services = {name: driver.DriverManager('quest.services', name, invoke_on_load='True').driver for name in web_services}

    if len(settings.get('USER_SERVICES', [])) > 0:
        for uri in settings.get('USER_SERVICES', []):
            try:
                drv = driver.DriverManager('quest.services', 'user', invoke_on_load='True', invoke_kwds={'uri': uri}).driver
                services['user-' + drv.name] = drv
            except Exception as e:
                logger.error('Failed to load local service from %s, with exception: %s' % (uri, str(e)))

    return services


def remove_key(d, key):
    r = dict(d)
    del r[key]
    return r


def jsonify(f):
    def wrapped_f(*args, **kw):
        if 'json' in list(kw.keys()):
            kw = json.loads(kw['json'])

        as_json = kw.get('as_json')
        if as_json:
            del kw['as_json']
            return json.dumps(f(*args, **kw), sort_keys=True, indent=4, separators=(',', ': '))
        else:
            return f(*args, **kw)
    return wrapped_f  # the wrapped function gets returned.


def stringify(args):
    return isinstance(args, list) and ','.join(args) or None


def to_dataframe(feature_collection):
    features = {}
    for feature in feature_collection['features']:
        data = feature['properties']
        coords = pd.np.array(feature['geometry']['coordinates'])
        if len(coords.shape) > 1:
            coords = coords.mean(axis=1)
        data.update({
            'geom_type': feature['geometry']['type'],
            'geom_coords': feature['geometry']['coordinates'],
            'longitude': coords.flatten()[0],
            'latitude': coords.flatten()[1],
            'service_id': feature['id']
        })

        features[feature['id']] = data
    return pd.DataFrame.from_dict(features, orient='index')


def to_geodataframe(feature_collection):
    features = {}
    for feature in feature_collection['features']:
        data = feature['properties']
        data.update({
            'service_id': feature['id'],
            'geometry': shapely.geometry.shape(feature['geometry'])
        })

        features[feature['id']] = data
    return gpd.GeoDataFrame.from_dict(features, orient='index')


def to_json_default_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()


def to_geojson(df):
    _func = {
        'LineString': LineString,
        'Point': Point,
        'Polygon': Polygon,
    }
    features = []
    if not df.empty:
        # TODO what is this code doing and is it now obsolete with the new DB?
        idx = df.columns.str.startswith('_')
        r = {field: field[1:] for field in df.columns[idx]}
        for uid, row in df.iterrows():
            metadata = json.loads(row[~idx].dropna().to_json())
            row = row[idx].rename(index=r)

            # create geojson geometry
            geometry = None
            if row['geom_type'] is not None:
                coords = row['geom_coords']
                if not isinstance(coords, (list, tuple)):
                    coords = json.loads(coords)
                geometry = _func[row['geom_type']](coords)
            del row['geom_type']
            del row['geom_coords']

            # split fields into properties and metadata
            properties = json.loads(row.dropna().to_json())
            properties.update({'metadata': metadata})
            features.append(Feature(geometry=geometry, properties=properties,
                            id=uid))

    return FeatureCollection(features)


def uuid(resource_type):
    """Generate new uuid.

    First character of uuid is replaced with 'f' or 'd' respectively
    for resource_type feature, dataset respectovely

    args:
        resource_type (string): type of resource i.e. 'feature' or 'dataset'

    returns:
        uuid (string)
    """
    uuid = uuid4().hex

    if resource_type == 'feature':
        uuid = 'f' + uuid[1:]

    if resource_type == 'dataset':
        uuid = 'd' + uuid[1:]

    return uuid


def is_uuid(uuid):
    """Check if string is a uuid4

    Validate that a UUID string is in fact a valid uuid4.
    source: https://gist.github.com/ShawnMilo/7777304
    """

    try:
        val = UUID(uuid, version=4)
    except ValueError:
        # If it's a value error, then the string is not a valid UUID.
        return False

    # If the uuid_string is a valid hex code, but an invalid uuid4,
    # the UUID.__init__ will convert it to a valid uuid4.
    # This is bad for validation purposes.
    return val.hex == uuid


def _abs_path(path, mkdir=True):
    if not os.path.isabs(path):
        path = os.path.join(get_quest_dir(), path)

    if mkdir:
        mkdir_if_doesnt_exist(path)

    return path
