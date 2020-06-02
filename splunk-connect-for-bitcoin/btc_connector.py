#!/usr/bin/python3

'''
btc_connector.py:

Splunk Connect for Bitcoin by AnChain.AI.
'''

__author__ = 'Dot Cheng, TianYi Zhang, Wei Quan'
__copyright__ = 'Copyright 2020, AnChain.AI'
__credits__ = ['Dot Cheng', 'TianYi Zhang', 'Wei Quan']
__license__ = 'GPL'
__version__ = '1.0.1'
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
logging.basicConfig(level=logging.INFO)


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
	"time_to_load": 1589418000,
	"file_name": "transactions"
}'
'''
def load_data_api(api_key, time_to_load, file_name):
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        'apikey': api_key,
        'time_to_load': time_to_load,
        'file_name': file_name
    }
    response = requests.post(anchain_btc_data_host, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        logging.error(f'request file ({file_name}) failed, response code: {response.status_code}')
        return None

    content_type = response.headers['Content-Type']

    if content_type == 'application/json':
        err_msg = json.loads(response.text)['err_msg']
        logging.warning(f'load data from server failed, err_msg: {err_msg}')
        return None

    if content_type != 'application/zip':
        logging.warning(f'invalid content type: {content_type}')
        return None

    return response.content


def load_data_local(api_key, time_to_load, file_name):
    data = load_data_api(api_key, time_to_load, file_name)
    logging.info(f'load transaction {file_name} from server, file size: {len(data) if data else 0}')
    if not data:
        return None

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
            return None

        with open(local_file, 'r') as f:
            data = f.readlines()
            logging.info(f'decode transaction {file_name} from server, record count: {len(data)}')
            return data


def load_transactions(api_key, time_to_load):
    def line_2_json(data):
        for line in data:
            '''
            you can remove unused field
            '''
            yield json.loads(line)

    # btc-txn-abbr
    data = load_data_local(api_key, time_to_load, 'transactions')
    if data:
        send_splunk(get_session(), line_2_json(data), 'btc-txn-abbr')

    # btc-txn-inout-addr-flat
    data = load_data_local(api_key, time_to_load, 'txn_inout_abbr_flat')
    if data:
        send_splunk(get_session(), line_2_json(data), 'btc-txn-inout-addr-flat')


if __name__ == '__main__':
    load_config('config.yml')
    while():
        time_to_load = int(time.time()) // 600 * 600
        time_to_load = datetime.fromtimestamp(time_to_load).astimezone(timezone.utc).isoformat()
        load_transactions(api_key, time_to_load)
        logging.info(f'load {sys.argv[1]} transactions finished.')
        time.sleep(600)
