import os
import logging
import time
import json

from helpers import APIHandler

import sqs

from data_parsers.helper_classes import FoursquareDetails

FOURSQUARE_CLIENT_ID = os.getenv('FOURSQUARE_CLIENT_ID')
FOURSQUARE_CLIENT_SECRET = os.getenv('FOURSQUARE_CLIENT_SECRET')
BOTO_QUEUE_NAME_FS_DETAILS = 'fs_details_queue'
BOTO_QUEUE_NAME_FS_MENU = 'fs_menu_details_queue'


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


def make_url(data):
    url = "https://api.foursquare.com/v2/venues/{}/menu?client_id={}&client_secret={}&v=20170109".format(
        data.fs_venue_id, FOURSQUARE_CLIENT_ID, FOURSQUARE_CLIENT_SECRET)
    return url


def make_request(queue, message):
    api_data = APIHandler(message)
    parsed_data = FoursquareDetails(api_data)
    if parsed_data.has_menu and parsed_data.has_venues:
        url = make_url(parsed_data)
        data = {
            'url': url,
            'fs_venue_id': parsed_data.fs_venue_id
        }
        send_message(queue, json.dumps(data))


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
    fs_details_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_DETAILS)
    menu_queue = sqs.get_queue(BOTO_QUEUE_NAME_FS_MENU)
    while True:
        message = get_message(fs_details_queue)
        if not message:
            time.sleep(5)
            continue
        make_request(menu_queue, message.body)
        delete_message(fs_details_queue, message)


if __name__ == '__main__':
    logging.basicConfig(level=10, format='%(asctime)s:{}'.format(logging.BASIC_FORMAT))
    try:
        run()
    except Exception as e:
        logging.exception(e)
        raise
