# Need 3 instances running.
import os
import logging
import time
import json
from contextlib import closing
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from helpers import APIHandler, FoursquareSession, delete_message

import sqs

from data_parsers.helper_classes import FoursquareDetails

BOTO_QUEUE_NAME_FS_DETAILS = 'fs_details_queue'
BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

DELETE_QUERY = """
DELETE FROM happyfinder_schema.happyfinder
WHERE fs_venue_id = :fs_venue_id;
"""

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)


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


def make_url(data, credentials):
    url = "https://api.foursquare.com/v2/venues/{}/menu?client_id={}&client_secret={}&v=20170109".format(
        data.fs_venue_id, credentials.foursquare_client_id, credentials.foursquare_client_secret)
    return url


def delete(fs_venue_id):
    with closing(Session()) as s:
        try:
            s.execute(DELETE_QUERY, params={'fs_venue_id': fs_venue_id})
        except Exception:
            s.rollback()
            raise
        s.commit()


def make_request(queue, message, credentials):
    api = APIHandler(message)
    api_data = api.get_load()
    parsed_data = FoursquareDetails(api_data)
    if parsed_data.has_menu:
        url = make_url(parsed_data, credentials)
        data = {
            'url': url,
            'category': parsed_data.category,
            'fs_venue_id': parsed_data.fs_venue_id
        }
        send_message(queue, json.dumps(data))
    else:
        delete(parsed_data.fs_venue_id)


def run():
    fs_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = get_message(fs_details_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        logging.info('menu queue: {}\nmessage.body:{}'.format(menu_queue, message.body))
        make_request(menu_queue, message.body, credentials)
        delete_message(fs_details_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    s = FoursquareSession(version='20170109')
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
