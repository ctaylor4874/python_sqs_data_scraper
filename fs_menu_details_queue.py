import os
import logging
import time
import json
from contextlib import closing
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from helpers import APIHandler

import sqs

from helpers import delete_message, FoursquareSession
from data_parsers.helper_classes import FoursquareVenueDetails

BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)

UPDATE_QUERY = """
UPDATE happyfinder_schema.happyfinder SET
happy_hour_string = :happy_hour_string,
fs_venue_id = :fs_venue_id,
category = :category
WHERE google_id = :google_id;
"""

DELETE_QUERY = """
DELETE FROM happyfinder_schema.happyfinder
WHERE google_id = :google_id;
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


def get_data(fs_venue_id):
    response = s.get("https://api.foursquare.com/v2/venues/{}/menu?".format(
        fs_venue_id))
    str_response = response.content.decode('utf-8')
    fs_data = FoursquareVenueDetails(json.loads(str_response))
    return fs_data


def parse_data(data):
    google_id = data.get('google_id')
    fs_venue_id = data.get('fs_venue_id')
    category = data.get('category')
    parsed_data = get_data(fs_venue_id)
    with closing(Session()) as conn:
        try:
            if parsed_data.happy_hour_string and fs_venue_id:
                conn.execute(UPDATE_QUERY, params={
                    'happy_hour_string': parsed_data.happy_hour_string.encode(
                        'utf-8') if parsed_data.happy_hour_string else None,
                    'fs_venue_id': fs_venue_id.encode('utf-8') if fs_venue_id else None,
                    'category': category.encode('utf-8') if category else None,
                    'google_id': google_id.encode('utf-8')
                })
            else:
                conn.execute(DELETE_QUERY, params={'google_id': google_id})
        except Exception as err:
            conn.rollback()
            raise
        else:
            conn.commit()


def run():
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = get_message(menu_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(30)
            continue
        data = json.loads(message.body)
        parse_data(data)
        delete_message(menu_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    s = FoursquareSession(version='20170109')
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
