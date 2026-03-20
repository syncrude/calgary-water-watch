#!/usr/bin/env python3
"""
Fetch all features from the CEAS Water Demand Daily FeatureServer table (id 15)
and write to ceas_daily_fallback.json in the workspace.
"""
import json
import urllib.request
import urllib.parse

BASE = 'https://services1.arcgis.com/AVP60cs0Q9PEA8rH/arcgis/rest/services/CEAS_%E2%80%93_Water_Demand_Daily/FeatureServer/15/query'
BATCH = 2000

def fetch_batch(offset):
    params = {
        'where': "TOTAL_CALGARY_ML IS NOT NULL",
        'outFields': 'METRIC_DATE,TOTAL_CALGARY_ML',
        'orderByFields': 'METRIC_DATE ASC',
        'resultOffset': str(offset),
        'resultRecordCount': str(BATCH),
        'f': 'json'
    }
    url = BASE + '?' + urllib.parse.urlencode(params)
    # print('GET', url)
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def main():
    offset = 0
    all_features = []
    while True:
        print(f'Fetching offset {offset}...')
        data = fetch_batch(offset)
        features = data.get('features') or []
        print(f'  got {len(features)} features')
        if not features:
            break
        all_features.extend(features)
        if len(features) < BATCH:
            break
        offset += BATCH
    out = {'features': all_features}
    with open('ceas_daily_fallback.json', 'w') as f:
        json.dump(out, f, indent=2)
    print(f'Wrote {len(all_features)} features to ceas_daily_fallback.json')

if __name__ == '__main__':
    main()
