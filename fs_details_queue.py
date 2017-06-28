"""
Loads message from the venue details queue.

Once a message has been received it makes the api request and parses the data to build the next
message to send to menu details queue.
"""
import os
import logging
import time
import json
from contextlib import closing
import sqlalchemy
from sqlalchemy.orm import sessionmaker

import sqs
from helpers import APIHandler, Alternator
from data_parsers.helper_classes import FoursquareDetails

BOTO_QUEUE_NAME_FS_DETAILS = 'fs_details_queue'
BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

DELETE_QUERY = """
DELETE FROM happyfinder_schema.happyfinder
WHERE fs_venue_id = :fs_venue_id;
"""

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)


def make_url(data, credentials):
    """
    URL generator to get menu details from Foursquare.

    :param data: Information about the venue.
    :param credentials: Foursquare credentials.
    :return: Generated URL.
    """
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
    """
    Creates an API object base on the message and gets the data.  Calls parsers and handlers
    for the api response and sends message to fs menu details queue if necessary.

    If the api response does not contain a menu, the script will delete the existing entry from the
    database.  If it does have a menu the script with generate the url to get the menu details and
    send the url along with some additional information to Foursquare menu details queue.

    :param queue: Foursquare menu details queue.
    :param message: Message received from Foursquare details queue.
    :param credentials: Foursquare credentials.
    :return:
    """
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
        sqs.send_message(queue, json.dumps(data))
    else:
        delete(parsed_data.fs_venue_id)


def run():
    credentials_alternator = Alternator()
    fs_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = sqs.get_message(fs_details_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        logging.info('menu queue: {}\nmessage.body:{}'.format(menu_queue, message.body))
        credentials = credentials_alternator.toggle_foursquare_values()
        make_request(menu_queue, message.body, credentials)
        sqs.delete_message(fs_details_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=20, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
