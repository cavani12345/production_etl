"""Connector and methods accessing S3"""
import os

import boto3
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime,timedelta
import logging


class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                      aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=os.environ[endpoint_url])
        self._bucket = self._s3.Bucket(os.environ[bucket])
        self._logger = logging.getLogger(__name__)

    def list_files_in_prefix(self):
        bucket_obj1 = self._bucket.objects.filter(Prefix='2022-01-31')
        files = [obj.key for obj in bucket_obj1]
        return files

    def read_csv_to_df(self,key, decoding = 'utf-8', sep = ','):
        self._logger.info('Readinf File %s/%s/%s',self.endpoint_url,self._bucket,key)
        csv_obj =self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self,df,key):

        if df.empty:
            self._logger.info("Empty dataframe was writen")
            return None
        
        self._logger.info("Writing file %s/%s/%s/",self.endpoint_url,self._bucket,key)
        out_buffer = BytesIO()
        df.to_csv(out_buffer, index=False)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
