import os
import requests
import json
import logging
import time
from itertools import cycle
from collections import namedtuple

FOURSQUARE_CLIENT_ID = os.getenv('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.getenv('FOURSQUARE_CLIENT_SECRET')
SECONDARY_FOURSQUARE_CLIENT_ID = os.getenv('SECONDARY_FOURSQUARE_CLIENT_ID')
SECONDARY_FOURSQUARE_CLIENT_SECRET = os.getenv('SECONDARY_FOURSQUARE_SECRET')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

fs_credentials = namedtuple('Row', ['foursquare_client_id', 'foursquare_client_secret'])


class Alternator:
    """
    A class to alternate through credentials.
    """

    def __init__(self):
        self.alternator = cycle((
            fs_credentials(SECONDARY_FOURSQUARE_CLIENT_ID, SECONDARY_FOURSQUARE_CLIENT_SECRET),
            fs_credentials(FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET)
        ))

    def toggle_foursquare_values(self):
        return next(self.alternator)


class APIHandler:
    """
    Class to handle API calls and responses.

    Contains methods to check Foursquare response to see if my query limit has been reached.
    """

    def __init__(self, url):
        self.url = url

    def get_load(self):
        """
        Makes the API call to the requested url and checks the response.

        :return: Parsed json response from requested URL.
        """
        res = requests.get(self.url)
        self.check_response(res)
        str_response = res.content.decode('utf-8')
        return json.loads(str_response)

    def check_response(self, res):
        """
        Method to check the response to see if the query limit has been reached.

        If the query limit has been reached the method will sleep until the specified reset time.
        :param res: API response.
        :return:
        """
        if 'foursquare' in self.url and res.status_code == 403:
            reset_time = float(res.headers.pop('X-RateLimit-Reset'))
            logging.info('Rate Limit will Reset at: {}'.format(reset_time))
            while time.time() < reset_time:
                time.sleep(5)
