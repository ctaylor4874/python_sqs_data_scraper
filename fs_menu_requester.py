import os
import logging
import time
import json
import contextlib
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from helpers import APIHandler

import sqs

from data_parsers.helper_classes import FoursquareVenueDetails

FOURSQUARE_CLIENT_ID = os.getenv('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.getenv('FOURSQUARE_CLIENT_SECRET')
BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)

INSERT_QUERY = """
UPDATE happyfinder_schema.happyfinder SET
happy_hour_string = :happy_hour_string
WHERE fs_venue_id = :fs_venue_id;
"""

DELETE_QUERY = """
DELETE FROM happyfinder_schema.happyfinder
WHERE fs_venue_id = :fs_venue_id
LIMIT 1;
"""


def get_message(queue):
    message = queue.receive_messages(MaxNumberOfMessages=1)
    if not message:
        return None
    return message[0]


def check_errors(response):
    if response.get('ResponseMetadata', '').get('HTTPStatusCode', '') is not 200:
        logging.info('ERROR! {}'.format(response))
    return response


def make_request(queue, data):
    api_data = APIHandler(data.get('url', ''))
    fs_venue_id = data.get('fs_venue_id', '')
    parsed_data = FoursquareVenueDetails(api_data)
    with contextlib.closing(Session()) as s:
        try:
            if parsed_data.happy_hour_string:
                s.execute(INSERT_QUERY, params={
                    'happy_hour_string': parsed_data.happy_hour_string,
                    'fs_venue_id': fs_venue_id
                })
            else:
                s.execute(DELETE_QUERY, params={'fs_venue_id': fs_venue_id})
        except Exception as err:
            logging.info(err)
            raise


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


def run():
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = get_message(menu_queue)
        if not message:
            time.sleep(5)
            continue
        data = json.loads(message.body)
        make_request(menu_queue, data)
        delete_message(menu_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=10, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
