import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from app.core.config import settings
from .aws_manager import AWSManager

logger = logging.getLogger(__name__)
SQS_QUEUE_URL = settings.sqs_queue_url


class SQSQueueManager(AWSManager):
    def __init__(self, queue_url: Optional[str] = None, queue_name: Optional[str] = None, region: Optional[str] = None):
        # Initialize the parent AWSManager with SQS service
        super().__init__('sqs', region)
        
        # Determine queue_url
        if queue_url:
            self.queue_url = queue_url
        elif queue_name:
            try:
                response = self.client.get_queue_url(QueueName=queue_name)
                self.queue_url = response['QueueUrl']
            except (BotoCoreError, ClientError) as e:
                self._handle_aws_error(e, f"get queue URL for {queue_name}")
                raise
        else:
            self.queue_url = SQS_QUEUE_URL
            if not self.queue_url:
                raise ValueError('SQS_QUEUE_URL must be set as an environment variable, or pass queue_url or queue_name to SQSQueueManager.')

    def receive_messages(self, max_number: int = 1, wait_time: int = 10, visibility_timeout: int = 30) -> List[Dict[str, Any]]:
        try:
            response = self.client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_number,
                WaitTimeSeconds=wait_time,  # Long polling
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=['All']
            )
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages from SQS.")
            return messages
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error while receiving message:", self.queue_url)
            self._handle_aws_error(e, "receive messages")
            return []
        
    def delete_message(self, receipt_handle: str) -> bool:
        try:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted from SQS.")
            return True
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, "delete message")
            return False

    def poll_queue(self, handler: Callable[[Dict[str, Any]], None], poll_interval: int = 1):
        """
        Continuously poll the SQS queue and process messages with the handler function.
        handler: function that takes a message dict and processes it.
        poll_interval: seconds to wait between polls if no messages are found.
        """
        logger.info("Starting SQS queue polling...")
        while True:
            messages = self.receive_messages(max_number=10, wait_time=10)
            if not messages:
                time.sleep(poll_interval)
                continue
            for message in messages:
                try:
                    handler(message)
                    self.delete_message(message['ReceiptHandle'])
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
    
    def create_fifo_queue(self, queue_name: str ):
        try:
            if not queue_name.endswith('.fifo'):
                raise ValueError("FIFO queue name must end with '.fifo'")
           
            try:
                response = self.client.get_queue_url(QueueName=queue_name)
                queue_url = response['QueueUrl']
                logger.info(f"Queue already exists: {queue_url}")
                self.queue_url = queue_url
                return queue_url
           
            except self.client.exceptions.QueueDoesNotExist:
                pass
           
            attributes = {
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'true'
            }
 
            response = self.client.create_queue(
                QueueName=queue_name,
                Attributes=attributes
            )
 
            queue_url = response['QueueUrl']
            self.queue_url = queue_url
            return queue_url
 
        except (BotoCoreError, ClientError, ValueError) as e:
            self._handle_aws_error(e, f"create FIFO queue {queue_name}")
            return None
    
def send_message(self, message_body: str, message_attributes: Optional[Dict[str, Any]] = None, message_group_id: Optional[str] = None) -> Optional[str]:
        try:
            params = {
                'QueueUrl': self.queue_url,
                'MessageBody': message_body
            }
            if message_attributes:
                params['MessageAttributes'] = message_attributes
            if message_group_id:
                params['MessageGroupId'] = message_group_id
            response = self.client.send_message(**params)
            logger.info(f"Message sent to SQS: {response.get('MessageId')}")
            return response.get('MessageId')
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, "send message")
            return None


# Example usage:
# def process_message(msg):
#     print("Processing message:", msg['Body'])
#
# if __name__ == "__main__":
#     manager = SQSQueueManager(queue_name='my-queue-name')
#     manager.send_message("Hello SQS!")
#     manager.poll_queue(process_message) 
