import os
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from typing import Optional, Dict, Any


logger = logging.getLogger(__name__)

class AWSManager:
    """
    Base class for AWS service managers.
    Provides common AWS configuration and client setup.
    """
    
    def __init__(
        self,
        service_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: Optional[str] = None
    ):
        """
        Initialize AWS manager with common configuration.
        
        Args:
            service_name: The AWS service name (e.g., 'sqs', 's3', 'dynamodb')
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region: AWS region (optional)
        """
        self.service_name = service_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = region or os.environ.get('AWS_REGION', 'us-east-1')
        
        # Validate required credentials
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ValueError(f'AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set for {service_name} manager')
        
        # Create AWS client
        self.client = boto3.client(
            service_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )
        
        logger.info(f"Initialized {service_name} client for region {self.aws_region}")
    
    def _handle_aws_error(self, error: Exception, operation: str) -> None:
        """
        Common error handling for AWS operations.
        
        Args:
            error: The AWS exception that occurred
            operation: Description of the operation that failed
        """
        if isinstance(error, (BotoCoreError, ClientError)):
            logger.error(f"AWS {self.service_name} {operation} failed: {error}")
        else:
            logger.error(f"Unexpected error during {self.service_name} {operation}: {error}")
    
    def get_client(self):
        """
        Get the AWS client instance.
        
        Returns:
            The boto3 client for the service
        """
        return self.client
    
    def get_region(self) -> str:
        """
        Get the AWS region being used.
        
        Returns:
            The AWS region name
        """
        return self.aws_region
    
    def test_connection(self) -> bool:
        """
        Test the AWS connection by making a simple API call.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # This is a generic approach - subclasses should override with service-specific tests
            if hasattr(self.client, 'list_queues'):  # SQS
                self.client.list_queues(MaxResults=1)
            elif hasattr(self.client, 'list_buckets'):  # S3
                self.client.list_buckets()
            elif hasattr(self.client, 'list_tables'):  # DynamoDB
                self.client.list_tables(Limit=1)
            else:
                # For other services, we'll assume connection is good if client was created
                return True
            return True
        except Exception as e:
            self._handle_aws_error(e, "connection test")
            return False 