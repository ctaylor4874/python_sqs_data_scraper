"""
Script to receive messages from the fs_menu_details_queue and either store happy hour data or remove row from database.
Once a message is received, the message is parsed and processed to prepare the data for the database.
"""
import os
import logging
import time
import json
from contextlib import closing
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from helpers import APIHandler
import sqs
from data_parsers.helper_classes import FoursquareVenueDetails

BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'

engine = sqlalchemy.create_engine(os.getenv('HAPPYFINDER_ENGINE'), encoding='utf8')
Session = sessionmaker(engine)

UPDATE_QUERY = """
UPDATE happyfinder_schema.happyfinder SET
happy_hour_string = :happy_hour_string,
category = :category
WHERE fs_venue_id = :fs_venue_id;
"""

DELETE_QUERY = """
DELETE FROM happyfinder_schema.happyfinder
WHERE fs_venue_id = :fs_venue_id;
"""


def parse_data(data):
    """
    Loads the url into the APIHandler. If there is a happy hour, update the database.  If not delete row.

    :param data: Dict received from SQS.
    :return:
    """
    api = APIHandler(data.get('url'))
    fs_venue_id = data.get('fs_venue_id')
    category = data.get('category')
    api_data = api.get_load()
    parsed_data = FoursquareVenueDetails(api_data)
    with closing(Session()) as s:
        try:
            if parsed_data.happy_hour_string:
                s.execute(UPDATE_QUERY, params={
                    'happy_hour_string': parsed_data.happy_hour_string.encode(
                        'utf-8') if parsed_data.happy_hour_string else None,
                    'category': category.encode('utf-8') if category else None,
                    'fs_venue_id': fs_venue_id
                })
            else:
                s.execute(DELETE_QUERY, params={'fs_venue_id': fs_venue_id})
        except Exception as err:
            s.rollback()
            raise
        s.commit()


def run():
    """
    Runner for the script.

    :return:
    """
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = sqs.get_message(menu_queue)
        if not message:
            logging.info(os.path.basename(__file__))
            time.sleep(5)
            continue
        data = json.loads(message.body)
        parse_data(data)
        sqs.delete_message(menu_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=30, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
