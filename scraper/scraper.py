from retry_requests import retry
from requests import Session
from datetime import date
from pathlib import Path
from bs4 import BeautifulSoup, SoupStrainer

import pandas as pd
import numpy as np

import json
import re
import shutil


today = date.today()
year = today.year

data_output_path = Path('data')

# remove old files
if data_output_path.is_dir():
    shutil.rmtree(data_output_path)

# mkdir
data_output_path.mkdir(parents=True, exist_ok=True)

stichting_nice_url = 'https://www.stichting-nice.nl'

stichting_nice_license = f"""
    vermeld dat het artikel van www.stichting-nice.nl komt,
    vermeld dat het artikel copyright 1996-{year} van Stichting NICE is,
    vermeld een duidelijke en werkende link naar de juiste pagina op de website van Stichting NICE.
"""

scrape_urls = {
    'icu': 'https://www.stichting-nice.nl/covid-19-op-de-ic.jsp',
    'clinic': 'https://www.stichting-nice.nl/covid-19-op-de-zkh.jsp',
    'hist/icu': 'https://www.stichting-nice.nl/covid-19-op-de-ic-history.jsp',
    'hist/clinic': 'https://www.stichting-nice.nl/covid-19-op-de-zkh-history.jsp',
}


def get(url):
    print(f'GET {url}')
    session = retry(Session(), retries=0, backoff_factor=0.2)

    ret = session.get(url)

    while ret.status_code != 200:
        print(f'STATUS {ret.status_code} retrying...')
        ret = session.get(url)

    return ret

def extract_covid19_js_urls(html):
    for link in BeautifulSoup(html, parse_only=SoupStrainer('script')):
        if link.has_attr('src') and link['src'].lower().strip().startswith('js/covid') and 'version=' in link['src'].lower():
            url = link['src'].strip()
            if not url[0] == '/':
                url = f'/{url}'

            return f'{stichting_nice_url}{url}'


def extract_covid19_data_urls(js):
    urls = re.findall("url[^']*'([^']*)'", js)

    retvals = []
    for url in urls:
        url = url.strip()
        if not url[0] == '/':
            url = f'/{url}'

        url = f'{stichting_nice_url}{url}'
        retvals.append(url)

    return list(set(retvals))

def read_nice_json(url):
    return get(url).json()

def save_json(data, jsonurl, outpath):
    data = {
        'data': data,
        'license': stichting_nice_license,
        'source': jsonurl,
    }

    with open(outpath, 'w') as fh:
        json.dump(data, fh)

def convert_to_df(data):
    if isinstance(data, list) and isinstance(data[0], list) and isinstance(data[0][0], list):
        # list of lists of lists
        df = pd.DataFrame(index=[])
        colcounter = 0
        for r1 in data:
            for idx, val in r1:
                df.at[idx, colcounter] = val
            colcounter += 1
    elif isinstance(data, dict) and isinstance(data[list(data.keys())[0]], list) and isinstance(data[list(data.keys())[0]][0], dict):
        # dict of lists of dicts
        df = pd.DataFrame(index=[])
        for col, recs in data.items():
            for rec in recs:
                df.at[rec['date'], col] = rec['value']
    elif isinstance(data, list) and isinstance(data[0], dict):
        # list of dicts
        df = pd.DataFrame(data)
        df = df.set_index(df.columns[0])
    elif isinstance(data, list) and isinstance(data[0], list) and isinstance(data[0][0], dict):
        # list of lists of dicts
        df = pd.DataFrame(index=[])
        colcounter = 0
        for col_idx in range(len(data)):
            df_col = pd.DataFrame(data[col_idx])
            df_col = df_col.set_index(df_col.columns[0])
            mappings = {}
            for col in df_col.columns:
                mappings[col] = colcounter
                colcounter += 1
            df_col.rename(columns=mappings, inplace=True)
            df = df.join(df_col, how="outer")
    elif isinstance(data, dict) and isinstance(data[list(data.keys())[0]], list) and isinstance(data[list(data.keys())[0]][0], list):
        # dict of lists
        print('dict of lists')
        df = pd.DataFrame(index=[])
        for col, records in data.items():
            df_col = pd.DataFrame(records, columns=['idx', col]).set_index('idx')
            df = df.join(df_col, how="outer")
    elif isinstance(data, dict) and not isinstance(data[list(data.keys())[0]], (list, dict)):
        # scalars
        df = pd.DataFrame.from_dict(data, orient='index', columns=['values'])
    else:
        df = pd.DataFrame(data)
    
    if df.index.name and 'date' in df.index.name.lower():
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
    if not df.isnull().values.any() and np.array_equal(df.values, df.values.astype(int)):
        df = df.astype(int)
    
    return df


for subpath, url in scrape_urls.items():
    outdir = data_output_path / subpath
    outdir.mkdir(parents=True, exist_ok=True)
    print(subpath, url, outdir)

    html = get(url).text
    jsurl = extract_covid19_js_urls(html)

    js = get(jsurl).text
    jsonurls = extract_covid19_data_urls(js)

    for jsonurl in jsonurls:
        fname = jsonurl.strip().rstrip('/').split('/')[-1]
        data = read_nice_json(jsonurl)
        save_json(data, jsonurl, outdir / f'{fname}.json')
        df = convert_to_df(data)

        df.to_csv(outdir / f'{fname}.csv')
        df.to_excel(outdir / f'{fname}.xlsx')
