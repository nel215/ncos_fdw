# coding:utf-8
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
import logging
import zlib
import json
import uuid
import boto3
from botocore.client import Config


class NCOSForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(NCOSForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.endpoint = options['endpoint']
        self.access_key = options['access_key']
        self.secret_key = options['secret_key']
        self.bucket = options['bucket']
        self.prefix = options['prefix']
        self.store_as = options['store_as']
        self.format = options['format']
        self.buffer = []
        log_to_postgres(message=str(options), level=logging.WARNING)

    def execute(self, quals, columns):
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint,
            config=Config(signature_version='s3'),
        )
        log_to_postgres(message=str(quals), level=logging.WARNING)
        log_to_postgres(message=str(columns), level=logging.WARNING)
        obj_list = s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)
        for c in obj_list['Contents']:
            res = s3.get_object(Bucket=self.bucket, Key=c['Key'])
            body = res['Body'].read()
            if self.store_as == 'gzip':
                body = zlib.decompress(body)
            for row in body.split('\n'):
                if self.format == 'json':
                    record = json.loads(row)
                else:
                    record = json.loads(row.split('\t')[2])
                yield record

    @property
    def rowid_column(self):
        return 'id'

    def insert(self, values):
        log_to_postgres(message=str(values), level=logging.WARNING)
        self.buffer.append(values)
        return {}

    def commit(self):
        if len(self.buffer) == 0:
            return

        s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint,
            config=Config(signature_version='s3'),
        )
        s3.create_bucket(
            Bucket=self.bucket,
        )
        if self.format == 'json':
            payload = '\n'.join(map(lambda row: json.dumps(row), self.buffer))
            log_to_postgres(message=payload, level=logging.WARNING)
            key = self.prefix + str(uuid.uuid4())
            s3.put_object(Bucket=self.bucket, Key=key, Body=payload)
        else:
            raise NotImplementedError(
                "This FDW does not support the {} format".format(self.format))
