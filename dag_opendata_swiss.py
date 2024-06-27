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
from airflow.models import Variable

log = logging.getLogger(__name__)

with DAG(
    dag_id='opendata_swiss_extract_data',
    schedule_interval='0 1 * * *',  # Every day at 01:00
    start_date=dt.datetime(2022, 8, 17),
    catchup=False,
    default_args=dict(
        email=Variable.get('AIRFLOW__MAIL_FAILURE'),
        email_on_failure=True,
    ),
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

        doc = return_dict
        with MongoHook('mongodb_u1082') as client:
            log.info(f'EXTRACT from DOC: {str(doc)[:100]}')
            for package in doc['data']:
                package['import_meta'] = import_meta
                client.insert_one('opendata_swiss_packages', package)

    t1 = extract_packages()
    t1
