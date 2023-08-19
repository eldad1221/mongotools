"""
Steps:
1. Fetch DB and backup conf from env
2. Download from S3 bucket last state
3. Alert if there is a collection in the DB that is not defined in one of the three Groups above
4. For each collection preform dump into local disk (in temp folder)
5. Upload dump file to S3 bucket
6. Write to S3 bucket the backup state file

Environment variables:
Mongo DB user, password, uri and DB name
Backup region
Backup bucket name
Backup conf for each collection
"""

import json
import boto3
from os import getenv
from sys import getsizeof
from quickbelog import Log
from datetime import datetime
from bson.json_util import dumps, loads
from quickbeutils import get_env_var_as_int
from iassessments import load_selected_env_variables
from iassessments.db import get_db_for_current_environment

DB_BACKUP_CONF_KEY = 'DB_BACKUP_CONF'
DB_BACKUP_BUCKET_NAME_KEY = 'DB_BACKUP_BUCKET_NAME'
DB_BACKUP_BULK_SIZE_LIMIT_KEY = 'DB_BACKUP_BULK_SIZE_LIMIT'

ENV_VARS_TO_LOAD = [
    'MONGO_URI', 'MONGO_USER', 'MONGO_PWD', 'MONGO_DB', DB_BACKUP_BUCKET_NAME_KEY, DB_BACKUP_CONF_KEY,
    DB_BACKUP_BULK_SIZE_LIMIT_KEY
]

load_selected_env_variables(env_vars=ENV_VARS_TO_LOAD)
APP_DB = get_db_for_current_environment()
DB_BACKUP_BUCKET_NAME = getenv(DB_BACKUP_BUCKET_NAME_KEY)
MONGO_DB_ID_FIELD = '_id'
DB_BACKUP_BULK_SIZE_LIMIT = get_env_var_as_int(DB_BACKUP_BULK_SIZE_LIMIT_KEY, 5 * 10 ** 6)

S3_RESOURCE = boto3.resource('s3')
S3_CLIENT = boto3.client('s3')


def put_s3_data(file_key: str, data: str):
    S3_RESOURCE.Bucket(DB_BACKUP_BUCKET_NAME).put_object(Key=file_key, Body=data)


def get_s3_data(file_key: str) -> str:

    result = S3_CLIENT.get_object(
        Bucket=DB_BACKUP_BUCKET_NAME,
        Key=file_key
    )
    return result['Body'].read().decode('utf-8')


FULL_BACKUP = 'full'
INCREMENTAL_BACKUP = 'incremental'


def get_conf() -> dict:
    conf_s = getenv(DB_BACKUP_CONF_KEY, '{}')
    conf = json.loads(conf_s)
    return conf


def check_missing_collections(conf: dict):

    db_collections = APP_DB.list_collection_names()
    existing_in_conf = list(conf.keys())
    missing_collections = [x for x in db_collections if x not in existing_in_conf]
    if len(missing_collections) > 0:
        msg = f'Add the following collections to {DB_BACKUP_CONF_KEY} env-var: {missing_collections}'
        raise NotImplementedError(msg)


LAST_ID_FILE_NAME = 'last_key.txt'


def get_last_saved_key(folder: str) -> str:
    return get_s3_data(file_key=f'{folder}/{LAST_ID_FILE_NAME}')


def save_last_id(folder: str, last_id: str, prefix: str = ''):
    put_s3_data(file_key=f'{folder}/{prefix}{LAST_ID_FILE_NAME}', data=last_id)


def get_collection_data(collection_name: str, query: dict, key_field: str = MONGO_DB_ID_FIELD) -> (str, str, int):
    count = 0
    data = '['
    is_first = True
    last_key = None
    for doc in APP_DB[collection_name].find(query):
        if is_first:
            is_first = False
        else:
            data += ', '
        data += f'{dumps(doc)}'
        last_key = dumps(doc.get(key_field))
        count += 1

        if getsizeof(data) > DB_BACKUP_BULK_SIZE_LIMIT:
            data += ']'
            yield data, last_key, count
            data = '['
            is_first = True
            last_key = None
    data += ']'
    yield data, last_key, count


def get_s3_file_key(file: str, folder: str) -> str:
    return f'{folder}/{file}.bak'


def backup_collection(collection_name: str, backup_type: str, backup_id: str, key_field: str = MONGO_DB_ID_FIELD):
    sw_id = Log.start_stopwatch(msg=f"Start backup {collection_name}")
    query = {}
    if backup_type == FULL_BACKUP:
        folder = backup_id
        file_prefix = f'{collection_name}-'
        file_key = get_s3_file_key(file=collection_name, folder=folder)
        Log.info(f'Full backup for collection {collection_name}.')
    elif backup_type == INCREMENTAL_BACKUP:
        folder = collection_name
        file_prefix = ''
        try:
            last_saved_key = get_last_saved_key(folder=folder)
            query[key_field] = {'$gt': loads(last_saved_key)}
        except Exception:
            last_saved_key = 'first document'
        file_key = get_s3_file_key(file=f'{collection_name}-{backup_id}', folder=folder)
        Log.info(f'Incremental backup for collection {collection_name}, starting from {last_saved_key}.')
    else:
        raise ValueError(f'Backup type {backup_type} is not supported.')
    file_count = 0
    for data, last_key, count in get_collection_data(collection_name=collection_name, query=query, key_field=key_field):
        file_count += 1
        put_s3_data(file_key=f'{file_key}{file_count:06d}', data=data)
        if last_key not in ['', None, 'None']:
            save_last_id(folder=folder, last_id=last_key, prefix=file_prefix)
        Log.debug(
            f'Saved {count} documents from {collection_name} ({backup_type}), last key = {last_key}, '
            f'in {Log.stopwatch_seconds(stopwatch_id=sw_id, print_it=False)} seconds.'
        )

    Log.stop_stopwatch(stopwatch_id=sw_id)
    return file_count


def backup(conf: dict):
    backup_id = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    for name, backup_conf in conf.items():
        backup_conf_tokens = backup_conf.strip().split('-')
        backup_type = backup_conf_tokens[0].strip().lower()

        if backup_type in ['f', FULL_BACKUP]:
            backup_collection(collection_name=name, backup_type=FULL_BACKUP, backup_id=backup_id)

        elif backup_type in ['i', INCREMENTAL_BACKUP]:
            if len(backup_conf_tokens) == 1:
                key_field = MONGO_DB_ID_FIELD
            else:
                key_field = backup_conf_tokens[1].strip()

            backup_collection(
                collection_name=name,
                backup_type=INCREMENTAL_BACKUP,
                backup_id=backup_id,
                key_field=key_field
            )


def restore_collection(collection_name: str, folder: str) -> int:
    result = S3_CLIENT.get_object(
        Bucket=DB_BACKUP_BUCKET_NAME,
        Key=f'{folder}/{collection_name}.bak'
    )
    data = result['Body'].read()
    documents = loads(data)
    count = 0
    collection = APP_DB.get_collection(name=f'{collection_name}_restored')
    for doc in documents:
        collection.insert_one(document=doc)
        count += 1

    Log.debug(f'{count} documents restored.')
    return count
