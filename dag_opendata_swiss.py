import datetime as dt
import itertools
import json
import logging
import math
import os

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

log = logging.getLogger(__name__)

with DAG(
    dag_id='opendata_swiss_extract_data',
    schedule_interval='0 1 * * *',  # Every day at 01:00
    start_date=dt.datetime(2022, 8, 17),
    catchup=False,
) as dag:

    timestamp = dt.datetime.now(tz=dt.timezone.utc)
    filename = f'{timestamp.strftime("%Y-%m-%d")}-opendataswiss-extract.json'
    full_filepath = '/tmp/' + filename
    remote_filepath = 'data/opendataswiss/'

    url = 'https://opendata.swiss/api/3/action/package_list'
    import_meta = {
        'timestamp_imported': timestamp.isoformat(),
        'extraction_url': url,
    }

    @task(task_id='extract_packages')
    def extract_packages():
        """Extracts data from packages from CKAN.

        Stores them as json in output/ directory.

        """

        # Count amount of packages in ckan
        PAGESIZE = 1000
        count_packages = requests.get(url)
        pages = math.ceil(len(json.loads(count_packages.text)['result']) / PAGESIZE)

        # Extract all packages and resources from ckan with paging
        pages_extract = []
        for i in range(pages):
            r = requests.get(
                'https://opendata.swiss/api/3/action/current_package_list_with_resources',
                params={
                    'limit': PAGESIZE,
                    'offset': i * PAGESIZE
                }
            )
            page_response = json.loads(r.text)
            pages_extract.append(page_response['result'])
        packages = list(itertools.chain(*pages_extract))
        log.info(f'Imported {len(packages)} packages from {url}.')
        return_dict = {
            'meta_data': import_meta,
            'data': packages,
        }

        # Store to /tmp before transfer
        with open(full_filepath, 'w') as file:
            json.dump(return_dict, file)

        # Store data directly after retrieval
        sftp_client = SFTPHook('sftp_nas69')
        sftp_client.store_file(remote_filepath + filename, full_filepath)

        os.remove(full_filepath)  # Remove temporary stored file from /tmp

    @task(task_id='load_to_mongo')
    def load_to_mongo():
        with MongoHook('mongodb_u1082') as client:
            sftp_client = SFTPHook('sftp_nas69')
            sftp_client.retrieve_file(remote_filepath + filename, full_filepath)
            with open(full_filepath, 'r') as file:
                doc = json.load(file)
                log.info(f'EXTRACT from DOC: {str(doc)[:100]}')
                for package in doc['data']:
                    package['import_meta'] = import_meta
                    client.insert_one('opendata_swiss_packages', package)
            os.remove(full_filepath)  # Remove temporary stored file from /tmp

    t1 = extract_packages()
    t2 = load_to_mongo()
    t1 >> t2
