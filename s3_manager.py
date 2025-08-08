import logging
from io import BytesIO, StringIO
from typing import Any, BinaryIO, Dict, List, Optional

import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

from app.shared_modules.aws_manager import AWSManager
from app.core.config import settings

logger = logging.getLogger(__name__)
S3_BUCKET_NAME = settings.s3_bucket_name


class S3Manager(AWSManager):
    """
    S3 service manager for handling S3 operations.
    Inherits from AWSManager for common AWS configuration.
    """
    
    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        """
        Initialize S3 manager.
        
        Args:
            bucket_name: Default bucket name for operations
            region: AWS region (defaults to AWS_REGION env var or 'us-east-1')
        """
        super().__init__('s3', region)
        self.bucket_name = bucket_name or self._get_default_bucket()
    
    def _get_default_bucket(self) -> str:
        """Get default bucket name from environment variable."""
        bucket = S3_BUCKET_NAME
        if not bucket:
            raise ValueError('S3_BUCKET_NAME must be set as an environment variable, or pass bucket_name to S3Manager.')
        return bucket
    
    def upload_file(self, file_path: str, object_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Upload a file to S3.
        
        Args:
            file_path: Local path to the file
            object_key: S3 object key (path in bucket)
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            True if upload successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        try:
            self.client.upload_file(file_path, bucket, object_key)
            logger.info(f"File uploaded to s3://{bucket}/{object_key}")
            return True
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"upload file {file_path} to {bucket}/{object_key}")
            return False
    
    def upload_fileobj(self, file_obj: BinaryIO, object_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Upload a file object to S3.
        
        Args:
            file_obj: File-like object to upload
            object_key: S3 object key (path in bucket)
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            True if upload successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        try:
            self.client.upload_fileobj(file_obj, bucket, object_key)
            logger.info(f"File object uploaded to s3://{bucket}/{object_key}")
            return True
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"upload file object to {bucket}/{object_key}")
            return False
    
    def multipart_upload_fileobj(self, file_obj: BinaryIO, object_key: str, bucket_name: Optional[str] = None, chunk_size: int = 5 * 1024 * 1024) -> bool:
        """
        Upload a file object to S3 using multipart upload (streaming large files).
        - chunk_size: in bytes (default 5MB, min allowed by S3)
        """
        bucket = bucket_name or self.bucket_name
        try:
            # Step 1: Create multipart upload
            response = self.client.create_multipart_upload(Bucket=bucket, Key=object_key)
            upload_id = response['UploadId']
            logger.info(f"Multipart upload started: UploadId={upload_id}")

            part_number = 1
            parts = []

            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break

                part_response = self.client.upload_part(
                    Body=chunk,
                    Bucket=bucket,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                )

                parts.append({
                    'PartNumber': part_number,
                    'ETag': part_response['ETag'],
                })

                logger.debug(f"Uploaded part {part_number}, ETag={part_response['ETag']}")
                part_number += 1

            # Step 2: Complete the multipart upload
            self.client.complete_multipart_upload(
                Bucket=bucket,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            logger.info(f"Multipart upload completed: s3://{bucket}/{object_key}")
            return True

        except Exception as e:
            logger.error(f"Multipart upload failed: {e}")
            # Abort the multipart upload if something goes wrong
            try:
                self.client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=object_key,
                    UploadId=upload_id
                )
                logger.info("Multipart upload aborted.")
            except Exception as abort_error:
                logger.error(f"Failed to abort multipart upload: {abort_error}")
            return False
    
    def download_file(self, object_key: str, file_path: str, bucket_name: Optional[str] = None) -> bool:
        """
        Download a file from S3.
        
        Args:
            object_key: S3 object key (path in bucket)
            file_path: Local path to save the file
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            True if download successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        try:
            self.client.download_file(bucket, object_key, file_path)
            logger.info(f"File downloaded from s3://{bucket}/{object_key} to {file_path}")
            return True
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"download file {bucket}/{object_key} to {file_path}")
            return False
    
    def delete_object(self, object_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Delete an object from S3.
        
        Args:
            object_key: S3 object key (path in bucket)
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            True if deletion successful, False otherwise
        """
        bucket = bucket_name or self.bucket_name
        try:
            self.client.delete_object(Bucket=bucket, Key=object_key)
            logger.info(f"Object deleted from s3://{bucket}/{object_key}")
            return True
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"delete object {bucket}/{object_key}")
            return False
    
    def list_objects(self, prefix: str = "", bucket_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with optional prefix.
        
        Args:
            prefix: Object key prefix to filter by
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            List of object information dictionaries
        """
        bucket = bucket_name or self.bucket_name
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            objects = response.get('Contents', [])
            logger.info(f"Listed {len(objects)} objects from s3://{bucket}/{prefix}")
            return objects
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"list objects in {bucket}/{prefix}")
            return []
    
    def generate_presigned_url(self, object_key: str, expiration: int = 3600, 
                              operation: str = 'get_object', bucket_name: Optional[str] = None) -> Optional[str]:
        """
        Generate a presigned URL for S3 object access.
        
        Args:
            object_key: S3 object key (path in bucket)
            expiration: URL expiration time in seconds (default: 1 hour)
            operation: S3 operation ('get_object', 'put_object', etc.)
            bucket_name: Bucket name (uses default if not provided)
            
        Returns:
            Presigned URL or None if generation failed
        """
        bucket = bucket_name or self.bucket_name
        try:
            url = self.client.generate_presigned_url(
                operation,
                Params={'Bucket': bucket, 'Key': object_key},
                ExpiresIn=expiration
            )
            logger.info(f"Generated presigned URL for s3://{bucket}/{object_key}")
            return url
        except (BotoCoreError, ClientError) as e:
            self._handle_aws_error(e, f"generate presigned URL for {bucket}/{object_key}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test S3 connection by listing buckets.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            self.client.list_buckets()
            return True
        except Exception as e:
            self._handle_aws_error(e, "connection test")
            return False 

    def read_tabular_file(self, file_type: str, object_key: str, bucket_name: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Read a CSV or Excel file from S3 into a Pandas DataFrame.

        Args:
            object_key: S3 object key (path in bucket)
            file_type: Type of file to read ('csv' or 'excel')
            bucket_name: Optional S3 bucket name (defaults to self.bucket_name)

        Returns:
            Pandas DataFrame if successful, None otherwise.
        """
        bucket = bucket_name or self.bucket_name
        try:
            response = self.client.get_object(Bucket=bucket, Key=object_key)
            content = response['Body'].read()
            if file_type == 'csv':
                decoded = content.decode('utf-8')
                df = pd.read_csv(StringIO(decoded))
            elif file_type == 'excel':
                df = pd.read_excel(BytesIO(content), engine='openpyxl')
            else:
                logger.error(f"Unsupported file type: {file_type}")
                return None
            logger.info(f"Excel file read from s3://{bucket}/{object_key}")
            return df
        except (BotoCoreError, ClientError, Exception) as e:
            self._handle_aws_error(e, f"read Excel from {bucket}/{object_key}")
            return None
