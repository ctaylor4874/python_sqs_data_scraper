# Need 3 instances running.
import os
import logging
import time
import json
from contextlib import closing
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from helpers import FoursquareSession, delete_message

import sqs

from data_parsers.helper_classes import FoursquareDetails

BOTO_QUEUE_NAME_FS_DETAILS = 'fs_details_queue'
BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)

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


def send_message(queue, url):
    response = queue.send_message(MessageBody=url)
    check_errors(response)


def get_fs_data(lat, lng, name):
    response = s.get('https://api.foursquare.com/v2/venues/search?intent=match&ll={},{}&query={}'.format(
        lat, lng, name
    ))
    str_response = response.content.decode('utf-8')
    fs_data = FoursquareDetails(json.loads(str_response))
    return fs_data


def delete(google_id):
    with closing(Session()) as conn:
        try:
            conn.execute(DELETE_QUERY, params={'google_id': google_id})
        except Exception as err:
            conn.rollback()
            raise
        else:
            conn.commit()


def make_request(queue, message):
    lat = message.get('lat')
    lng = message.get('lng')
    name = message.get('name')
    google_id = message.get('google_id')
    parsed_data = get_fs_data(lat, lng, name)

    if parsed_data.has_menu:
        data = {
            'fs_venue_id': parsed_data.fs_venue_id,
            'google_id': google_id,
            'category': parsed_data.category
        }
        send_message(queue, json.dumps(data))
    else:
        delete(google_id)


def run():
    fs_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = get_message(fs_details_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(30)
            continue
        message_data = json.loads(message.body)
        logging.debug('menu queue: {}\nmessage.body:{}'.format(menu_queue, message_data))
        make_request(menu_queue, message_data)
        delete_message(fs_details_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    s = FoursquareSession(version='20170109')
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
