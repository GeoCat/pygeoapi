import json
import logging
from re import compile
from uuid import UUID

import requests

from pygeoapi.provider.base import (
    BaseProvider,
    ProviderQueryError,
    ProviderConnectionError,
    ProviderNoDataError,
    ProviderInvalidQueryError,
    ProviderItemNotFoundError
)

ENCODED_JSON_REGEX = compile(r'("\"\".+?\"\"")')
LOGGER = logging.getLogger(__name__)


class GeoCoreProvider(BaseProvider):
    """ Provider for the Canadian Federal Geospatial Platform (FGP).

    Queries NRCan's geoCore API.
    """

    def __init__(self, provider_def):
        super().__init__(provider_def)

        LOGGER.debug('setting geoCore base URL')
        try:
            url = self.data['base_url']
        except KeyError:
            raise RuntimeError(
                f'missing base_url setting in {self.name} provider data'
            )
        else:
            # sanitize trailing slashes
            self._baseurl = f'{url.rstrip("/")}/'

        LOGGER.debug('map endpoints to provider methods')
        mapping = self.data.get('mapping', {})
        if not mapping:
            LOGGER.warning(f'No endpoint mapping found for {self.name} provider: using defaults')  # noqa
        self._query_url = f'{self._baseurl}{mapping.get(self.query.__name__, "geo")}'  # noqa
        self._get_url = f'{self._baseurl}{mapping.get(self.get.__name__, "id")}'

    @staticmethod
    def _parse_json(body):
        """ Parses the geoCore response body as a JSON object. """

        def unescape(match):
            """ Unescape string and replace double quotes with single ones. """
            bytes_ = match.group(0).encode('latin1')
            return bytes_.decode('unicode_escape').replace('""', '"').strip('"')

        result = {}
        if not body:
            return result

        # geoCore returns some JSON array values as encoded JSON strings
        # Python's JSON loader does not like them, so we have to replace those
        LOGGER.debug('parse JSON response body')
        json_str = ENCODED_JSON_REGEX.sub(unescape, body)
        try:
            result = json.loads(json_str)
        except json.JSONDecodeError as err:
            LOGGER.error('Failed to parse JSON response', exc_info=err)
        finally:
            return result

    @staticmethod
    def _valid_id(identifier):
        """ Returns True if the given identifier is a valid UUID. """
        try:
            str(UUID(identifier))
        except (TypeError, ValueError, AttributeError):
            return False
        return True

    def _request_json(self, url, params):
        """ Performs a GET request on `url` and returns the JSON response. """
        response = None
        try:
            response = requests.get(url, params)
            response.raise_for_status()
        except requests.HTTPError as err:
            LOGGER.error(err)
            raise ProviderQueryError(
                f'failed to query {response.url if response else url}')
        except requests.ConnectionError as err:
            LOGGER.error(err)
            raise ProviderConnectionError(
                f'failed to connect to {response.url if response else url}')

        LOGGER.debug(response.text)
        return self._parse_json(response.text)

    def _to_geojson(self, json_obj, skip_geometry=False):
        """ Turns a regular geoCore JSON object into GeoJSON. """
        features = []

        for item in json_obj.get('Items', []):
            feature = {
                'type': 'Feature',
                'geometry': None
            }
            # Pop ID an move it to top level
            id_ = item.pop('id')
            if not self._valid_id(id_):
                LOGGER.warning(f'skipped record with ID {id_}: not a UUID')
                continue
            feature['id'] = id_

            if not skip_geometry:
                # Remove coordinates from item and make Polygon geometry
                coords = item.pop('coordinates', [])
                if not isinstance(coords, list):
                    LOGGER.debug('try convert coordinates to an array')
                    try:
                        coords = json.loads(coords)
                    except json.JSONDecodeError as err:
                        LOGGER.warning(f'failed to parse coords: {err}')
                        coords = []
                if not coords:
                    LOGGER.debug('record has no geometry')
                else:
                    feature['geometry'] = {
                        'type': 'Polygon',
                        'coordinates': coords
                    }
            else:
                LOGGER.debug('skipped geometry')

            # Set properties and add to feature list
            feature['properties'] = item
            features.append(feature)

        if not features:
            raise ProviderNoDataError('query returned nothing')
        elif len(features) == 1:
            LOGGER.debug('returning single feature')
            return features[0]

        LOGGER.debug('returning feature collection')
        return {
            'type': 'FeatureCollection',
            'features': features
        }

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None):
        """
        Performs a geoCore search.

        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of 0..n GeoJSON features
        """
        params = {}

        if resulttype != 'results':
            # Supporting 'hits' will require a change on the geoCore API
            LOGGER.warning(f'Unsupported resulttype {resulttype}: '
                           f'defaulting to "results"')

        if bbox:
            LOGGER.debug('processing bbox parameter')
            minx, miny, maxx, maxy = bbox
            params['east'] = minx
            params['west'] = maxx
            params['north'] = maxy
            params['south'] = miny
        else:
            LOGGER.debug('set keyword_only search')
            params['keyword_only'] = 'true'

        # Set min and max (1-based!)
        LOGGER.debug('set query limits')
        params['min'] = startindex + 1
        params['max'] = startindex + limit

        LOGGER.debug(f'querying {self._query_url}')
        json_obj = self._request_json(self._query_url, params)

        LOGGER.debug(f'turn geoCore JSON into GeoJSON')
        return self._to_geojson(json_obj, skip_geometry)

    def get(self, identifier):
        """ Request a single geoCore record by ID. """
        LOGGER.debug('validating identifier')
        if not self._valid_id(identifier):
            raise ProviderInvalidQueryError(
                f'{identifier} is not a valid UUID identifier')

        params = {
            'id': identifier
        }

        LOGGER.debug(f'querying {self._get_url}')
        json_obj = self._request_json(self._get_url, params)

        if not json_obj.get('Items', []):
            raise ProviderItemNotFoundError(f'record id {identifier} not found')

        LOGGER.debug(f'turn geoCore JSON into GeoJSON')
        return self._to_geojson(json_obj)

    def __repr__(self):
        return f'<{self.__class__.__name__}> {self.data}'
