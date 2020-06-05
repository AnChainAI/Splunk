#!/usr/bin/python3

'''
btc_connector.py:

Splunk Connect for Bitcoin by AnChain.AI.
'''

__author__ = 'Dot Cheng, TianYi Zhang, Wei Quan'
__copyright__ = 'Copyright 2020, AnChain.AI'
__credits__ = ['Dot Cheng', 'TianYi Zhang', 'Wei Quan']
__license__ = 'GPL'
__version__ = '1.0.2'
__maintainer__ = 'Dot Cheng'
__email__ = 'dot.cheng@anchain.ai'
__status__ = 'Production'


import os
import sys
import json
import yaml
import logging
import requests
import time
from datetime import datetime, timezone
from dateutil import parser
from tempfile import TemporaryDirectory
import zipfile


# logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)-15s %(processName)s %(process)d %(thread)d %(module)s %(filename)s %(funcName)s %(lineno)d %(levelname)s : %(message)s',)


# global constant
FILE_TIME_LAST = './file_time_last'
TIME_LOAD_INTERVAL = 600

# global variable
api_key = None
splunk_btc_txn_index = None
splunk_http_collector_url = None
splunk_hec_token = None
anchain_btc_data_host = None

g_session = None


def load_config(file_name):
    global api_key
    global splunk_http_collector_url
    global splunk_btc_txn_index
    global splunk_hec_token
    global anchain_btc_data_host
    
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    conf = yaml.safe_load(open(file_name, 'r'))
    api_key = conf['apikey']
    splunk_http_collector_url = conf['splunk_http_collector_url']
    splunk_btc_txn_index = conf['splunk_btc_txn_index']
    splunk_hec_token = conf['splunk_hec_token']
    anchain_btc_data_host = conf['anchain_btc_data_host']


def get_session():
    global g_session
    if not g_session:
        g_session = requests.session()
    return g_session


def load_sourcetype(http_collector):
    _http_collector_sourcetype = {
        'btc-txn-abbr': 'st-btc-txn-abbr',
        'btc-txn-inout-addr-flat': 'st-btc-txn-inout-addr-flat'
    }
    return _http_collector_sourcetype[http_collector]


'''
curl -k https://localhost:8088/services/collector -H "Authorization: Splunk d66eb2d3-7ed1-47f0-bdfd-fab47fbb168f" -d '{
    "event": "hello world 3",
    "index": "btc_txns_v1",
    "sourcetype": "st-btc-txn-abbr",
    "time": 1589513057
}'
'''
def send_splunk(session, event_data, http_collector):
    if not event_data:
        return

    headers = {'Authorization': f'Splunk {splunk_hec_token}'}
    _bulk_data = []
    for _event in event_data:
        _bulk_data.append({'event': _event,
                           'time': _event['block_timestamp'],
                           'sourcetype': load_sourcetype(http_collector),
                           'index': splunk_btc_txn_index})
    if not _bulk_data:
        logging.warning('No data to send')
        return
        
    rep = session.post(splunk_http_collector_url, json=_bulk_data, headers=headers, verify=False)
    if rep.status_code != 200:
        raise Exception('Exception [{}]'.format(str(rep.text)))
        # logging.info('Exception [%s], failed event => %s', str(_event), str(rep))

    logging.info(f'send to splunk successful, record count: {len(_bulk_data)}')


'''
curl -s -X POST https://data.anchainai.com/btc_txns -H 'Content-Type: Application/json' --output 'x.zip' -d '{
    "apikey": "kkkkkkkkkkkkk",
    "time_last": -1,
    "time_current": 1589418000,
    "file_name": "transactions"
}'
'''
def load_data_api(api_key, time_last, time_to_load, file_name):
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        'apikey': api_key,
        'time_last': time_last,
        'time_current': time_to_load,
        'file_name': file_name
    }
    response = requests.post(anchain_btc_data_host, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        logging.error(f'request file ({file_name}) failed, response code: {response.status_code}')
        return None, None

    content_type = response.headers['Content-Type']

    if content_type == 'application/json':
        err_msg = json.loads(response.text)['err_msg']
        logging.warning(f'load data from server failed, err_msg: {err_msg}')
        return None, None

    if content_type != 'application/zip':
        logging.warning(f'invalid content type: {content_type}')
        return None, None

    time_last = None
    try:
        if 'time_last' in response.headers:
            time_last = int(response.headers['time_last'])
    except Exception as e:
        logging.warning(f'[EXCEPTON] when get time_last from response headers, msg: {str(e)}')

    return response.content, time_last

def load_data_local(api_key, time_last, time_to_load, file_name):
    # # TEST local
    # if file_name in ['transactions', 'txn_inout_abbr_flat']:
    #     with open(f'./cache/{file_name}.json', 'r') as f:
    #         data = f.readlines()
    #         logging.info(f'decode transaction {file_name} from server (fake), record count: {len(data)}')
    #         return data

    logging.info(f'load_data_api({time_last}, {time_to_load}, {file_name})')
    data, time_last_r = load_data_api(api_key, time_last, time_to_load, file_name)
    logging.info(f'load transaction "{file_name}" from server, file size: {len(data) if data else 0}')
    if not data:
        return None, -1

    with TemporaryDirectory() as tmpdir:
        local_file_zip = os.path.join(tmpdir, 'txns.zip')
        with open(local_file_zip, 'wb') as f:
            f.write(data)

        local_file = None
        with zipfile.ZipFile(local_file_zip, 'r') as zip:
            for fn in zip.namelist():
                if fn == f'{file_name}.json':
                    zip.extractall(path=tmpdir, members=[fn])
                    local_file = os.path.join(tmpdir, fn)
                    break
        if not local_file:
            logging.warning(f'invalid data file: {local_file_zip}')
            return None, -1

        with open(local_file, 'r') as f:
            data = f.readlines()
            logging.info(f'decode transaction {file_name} from server, record count: {len(data)}')
            return data, time_last_r


def load_transactions(api_key, time_to_load):
    ''' load transactions from data.anchainai.com and put it to your splunk index

    :param api_key:
    :param time_to_load:

    :return: 0-success, >0-need to catch up to newest, <0-failed
    '''

    def line_2_json(data):
        for line in data:
            '''
            remove unused field if you want
            '''
            yield json.loads(line)

    # align to 600 seconds
    time_to_load = int(time_to_load // TIME_LOAD_INTERVAL * TIME_LOAD_INTERVAL)

    # load time_last
    time_last = None
    try:
        if os.path.isfile(FILE_TIME_LAST):
            with open(FILE_TIME_LAST, 'r') as f:
                time_last = parser.parse(f.readline().strip(), ignoretz=False).timestamp()
    except Exception as e:
        logging.warning(f'[EXCEPTON] when load time_last, msg: {str(e)}')

    # btc-txn-abbr (free user)
    data, time_last_1 = load_data_local(api_key, time_last, time_to_load, 'transactions')
    if data:
        send_splunk(get_session(), line_2_json(data), 'btc-txn-abbr')

    # btc-txn-inout-addr-flat (registered user)
    data, _ = load_data_local(api_key, time_last, time_to_load, 'txn_inout_abbr_flat')
    if data:
        send_splunk(get_session(), line_2_json(data), 'btc-txn-inout-addr-flat')

    if not time_last_1 or time_last_1 <= 0:
        logging.warning(f'invalid return value of load_data_api(), time_last_return: {time_last_1}')
        return -1

    # update time_last
    try:
        with open(FILE_TIME_LAST, 'w') as f:
            time_write = datetime.fromtimestamp(time_last_1).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S%z')
            f.write(time_write)
            logging.info(f'update time_last({time_write}) to database')
    except Exception as e:
        logging.warning(f'[EXCEPTON] when write time_last, msg: {str(e)}')
        return -2

    return 0 if time_last_1 == time_to_load else 1


if __name__ == '__main__':
    load_config('config.yml')
    while True:
        time_to_load = int(datetime.utcnow().timestamp())
        load = load_transactions(api_key, time_to_load)
        logging.info(f'load_transaction({time_to_load}) = {load}')

        # load: 0-success, >0-need to catch up to newest, <0-failed
        time_sleep = 10 if load > 0 else TIME_LOAD_INTERVAL
        logging.info(f'sleep {time_sleep} seconds')
        time.sleep(time_sleep)
