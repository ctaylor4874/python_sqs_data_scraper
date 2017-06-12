import os
import requests
import json
import logging
import requests
import time
from urllib import parse
from itertools import cycle
from collections import namedtuple

FOURSQUARE_CLIENT_ID = os.getenv('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.getenv('FOURSQUARE_CLIENT_SECRET')
SECONDARY_FOURSQUARE_CLIENT_ID = os.getenv('SECONDARY_FOURSQUARE_CLIENT_ID')
SECONDARY_FOURSQUARE_CLIENT_SECRET = os.getenv('SECONDARY_FOURSQUARE_SECRET')
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

fs_credentials = namedtuple('Row', ['client_id', 'secret_key'])
alternator = cycle((
    fs_credentials(SECONDARY_FOURSQUARE_CLIENT_ID, SECONDARY_FOURSQUARE_CLIENT_SECRET),
    fs_credentials(FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET)
))
credentials = (fs_credentials(SECONDARY_FOURSQUARE_CLIENT_ID, SECONDARY_FOURSQUARE_CLIENT_SECRET),
               fs_credentials(FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET))


# class Credential(object):
#
#     def __init__(self, client_id, secret_key):
#         self.client_id = client_id
#         self.secret_key = secret_key

class Error(Exception):
    pass


class FSTimeoutError(Error):
    def __init__(self, creds=None):
        self.message = 'ID: {}, Secret: {}\nCurrent time later that reset time, still returned a 403.'.format(
                creds.client_id, creds.secret_key)


class FoursquareSession(requests.Session):
    def __init__(self, *args, **kwargs):
        self.credentials = credentials
        self.version = kwargs.pop('version', '')
        self._credentials = cycle(self.credentials)
        self.timeout = {}
        super(FoursquareSession, self).__init__()

    def next_credential(self):
        """
        :rtype: Credential
        :return:
        """
        return next(self._credentials)

    def get_timeout(self, creds, res):
        reset_time = float(res.headers.pop('X-RateLimit-Reset'))
        if self.timeout.get(creds.client_id, 0) < time.time():
            raise FSTimeoutError(creds=creds)
        self.timeout[creds.client_id] = reset_time

    def get(self, url, **kwargs):
        kwargs.setdefault('allow_redirects', True)

        creds = self.next_credential()
        extra_args = {
            'v': self.version,
            'client_secret': creds.secret_key,
            'client_id': creds.client_id
        }
        extra = parse.urlencode(extra_args)

        new_url = '{}&{}'.format(url, extra)

        response = self.request('GET', new_url, **kwargs)
        if response.status_code == 403:
            self.get_timeout(creds, response)
            self.get(url, **kwargs)

        return response


class Alternator:
    def __init__(self):
        self.alternator = cycle((
            fs_credentials(SECONDARY_FOURSQUARE_CLIENT_ID, SECONDARY_FOURSQUARE_CLIENT_SECRET),
            fs_credentials(FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET)
        ))

    def toggle_foursquare_values(self):
        return next(self.alternator)


class APIHandler:
    def __init__(self, url):
        self.url = url

    def get_load(self):
        res = requests.get(self.url)
        self.check_response(res)
        str_response = res.content.decode('utf-8')
        return json.loads(str_response)

    def check_response(self, res):
        if 'foursquare' in self.url and res.status_code == 403:
            reset_time = float(res.headers.pop('X-RateLimit-Reset'))
            logging.info('Rate Limit will Reset at: {}'.format(reset_time))
            while time.time() < reset_time:
                time.sleep(5)


def delete_message(queue, message):
    """
    Delete the message from the queue.

    See: http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Queue.delete_messages
    :return:
    """
    entries = [
        {
            'Id': message.message_id,
            'ReceiptHandle': message.receipt_handle
        }
    ]
    response = queue.delete_messages(Entries=entries)

    successful = response.get('Successful', [{}])[0].get('Id') == message.message_id
    if successful:
        return

    failure_details = response.get('Failed', [{}])
    failed_message_id = failure_details.get('Id')
    if failed_message_id != message.message_id:
        raise ValueError('Delete message was unsuccessful but failed message id does not match expected message id. '
                         'failed_message_id={} expected_message_id={}'.format(failed_message_id, message.message_id))

    raise ('Details: {}\nID: {}'.format(failure_details, failed_message_id))
