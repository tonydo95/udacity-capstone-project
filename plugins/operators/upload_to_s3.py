import os
import boto3
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class UploadToS3Operator(BaseOperator):
    """
    Operator that uploads all data files to S3 bucket
    """

    ui_color = '#218251'

    @apply_defaults
    def __init__(self,
                 s3_bucket_name,
                 folder_names,
                 input_path,
                 *args, **kwargs):
        
        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket_name = s3_bucket_name
        self.folder_names = folder_names
        self.input_path = input_path

    def execute(self, context):
        """
        Load files onto the S3 location
        """
        
        for folder in self.folder_names:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(self.s3_bucket_name)

            for root, dirs, files in os.walk(self.input_path + folder):
                for file in files:
                    full_path = os.path.join(root, file)
                    with open(full_path, 'rb') as data:
                        bucket.put_object(
                            Key=full_path[len(self.input_path):],
                            Body=data
                        )
        
        self.log.info('Succesfully uploaded!')
