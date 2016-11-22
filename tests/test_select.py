# coding:utf-8
import psycopg2
import boto3
from botocore.client import Config


def prepare():
    s3 = boto3.client(
        's3',
        aws_access_key_id='access_key',
        aws_secret_access_key='secret_key',
        endpoint_url='http://localhost:4569',
        config=Config(signature_version='s3'),
    )
    s3.create_bucket(
        Bucket='test',
    )
    payload = '\n'.join([
        '2016-11-11T11:11:11\ttag.example\t{"id": 1, "key": "v1"}',
        '2016-11-11T11:11:11\ttag.example\t{"id": 2, "key": "v2", "foo": "bar"}',  # nopep8
    ])
    s3.put_object(Bucket='test', Key='path/to', Body=payload.encode())


def test_select():
    prepare()
    con = psycopg2.connect('host=localhost port=5432 user=postgres')
    cur = con.cursor()
    cur.execute('DROP FOREIGN TABLE IF EXISTS ncos_example;')
    cur.execute('DROP SERVER IF EXISTS multicorn_ncos;')
    cur.execute('DROP EXTENSION IF EXISTS multicorn;')
    cur.execute('CREATE EXTENSION multicorn;')
    cur.execute("CREATE SERVER multicorn_ncos FOREIGN DATA WRAPPER multicorn\
                 options (wrapper 'ncos_fdw.NCOSForeignDataWrapper');")
    cur.execute("CREATE FOREIGN TABLE ncos_example (\
                   id integer,\
                   key varchar\
                 ) SERVER multicorn_ncos\
                 options (\
                   bucket 'test',\
                   prefix 'path',\
                   endpoint 'http://localhost:4569',\
                   access_key 'access_key',\
                   secret_key 'secret_key'\
                 );")
    con.commit()
    cur.execute("SELECT * FROM ncos_example")
    assert cur.fetchall() == [
        (1, "v1"),
        (2, "v2"),
    ]
