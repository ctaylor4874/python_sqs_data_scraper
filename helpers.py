import requests
import json
import logging
import time


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
