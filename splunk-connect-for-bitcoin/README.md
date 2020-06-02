# AnChain.AI Splunk Connect for Bitcoin.

# Install
```bash
- pip3 install -r requirements.txt
```

# Configuration
## Splunk Enterprise
- create Index for hosting btc transaction data
    - default index name: btc_txns_v1
- create HTTP Event Collector Token for receiving btc transaction data
    - default token name: anchain_bei_token
    - default token value: d66eb2d3-7ed1-47f0-bdfd-fab47fbb168f

## [config.yml](config.yml)
- apikey: required for fetch data from data.anchainai.com. You can also sign up for your own API Key and get more features on https://bei.anchainai.com
- splunk_btc_txn_index: the Splunk index you created for hosting the btc transaction data
- splunk_http_collector_url: the Splunk Http Event Collector url of your Splunk instance
- splunk_hec_token: the Splunk Http Event Collector Token

# Usage
```bash
python3 btc_connector.py
```

