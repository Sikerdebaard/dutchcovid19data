#!/usr/bin/env python
# coding: utf-8

# In[18]:


import sys

get_ipython().system('{sys.executable} -m pip install requests retry-requests openpyxl xlwt pandas')


# In[4]:


from retry_requests import retry
from requests import Session

def get(url):
    session = retry(Session(), retries=10, backoff_factor=0.2)
    
    ret = session.get(url)
    
    while ret.status_code != 200:
        print('asd')
        ret = session.get(url)
    
    return ret


# In[58]:


from datetime import date
from pathlib import Path
import json
import pandas as pd

today = date.today()



data_output_path = Path('./data')
data_output_path.mkdir(parents=True, exist_ok=True)

# remove old files
for f in data_output_path.glob('*'):
    f.unlink()

stichting_nice_url = 'https://www.stichting-nice.nl'

stichting_nice_license = """
    vermeld dat het artikel van www.stichting-nice.nl komt,
    vermeld dat het artikel copyright 1996-2020 van Stichting NICE is,
    vermeld een duidelijke en werkende link naar de juiste pagina op de website van Stichting NICE.
"""

expected_mappings = [
    '/covid-19/public/global',
    '/covid-19/public/new-intake/',
    '/covid-19/public/intake-count/',
    '/covid-19/public/intake-cumulative/',
    '/covid-19/public/ic-count/',
#    '/covid-19/public/age-distribution-died-and-survivors/',
    '/covid-19/public/age-distribution-status/',
#    '/covid-19/public/age-distribution/',
    '/covid-19/public/died-and-survivors-cumulative/',
    '/covid-19/public/behandelduur-distribution/',
]


def alt_distribution_to_xlsx(data, output_file, columns, index_label='age_group'):
    df_out = None
    counter = 0

    for s in data:
        if df_out is None:
            df_out = pd.DataFrame(s)
            df_out.set_index(0, inplace=True)
            df_out.rename(columns={1: columns[counter]}, inplace=True)
        else:
            df_tmp = pd.DataFrame(s).set_index(0)
            df_tmp.rename(columns={1: columns[counter]}, inplace=True)
            df_out = df_out.join(df_tmp)
        counter += 1

    df_out.to_excel(output_file, index_label=index_label)

def distribution_to_xlsx(data, output_file):
    mapped = {}
    for distribution in data:
        for group, perc in distribution:
            if group not in mapped:
                mapped[group] = []
            mapped[group].append(perc)

    df = pd.DataFrame.from_dict(data=mapped, columns=['died', 'survived'], orient='index')

    df.to_excel(output_file, index_label='age_group')

    
def global_to_xlsx(data, output_file):
    df = pd.DataFrame.from_dict(data, orient='index')
    
    df.to_excel(output_file, header=False)
    
def alt_date_based_data_to_xlsx(data, output_file, names):
    df = None
    
    counter = 0
    for d in data:
        if df is None:
            df = date_based_data_to_xlsx(d, output_file, False)
            df.rename({'value': names[counter]}, axis=1, inplace=True)
        else:
            df2 = date_based_data_to_xlsx(d, output_file, False)
            df2.rename({'value': names[counter]}, axis=1, inplace=True)
            df = df.join(df2, how='outer')
        counter += 1
    
    df.to_excel(output_file)
    
def date_based_data_to_xlsx(data, output_file, output=True):
    df = pd.DataFrame.from_dict(data)
    df['date'] = pd.to_datetime(df['date']).dt.date  # convert date to date-type 
    df = df.set_index('date').sort_index()  # set date as index and sort on date
    df = df.loc[:, (df != 0).any(axis=0)]  # remove all columns with only 0-values
    idx = pd.date_range(df.index.min(), df.index.max())  # reindex so that missing dates are added
    df = df.reindex(idx)
    df = df.sort_index()  # sort by index
    
    for column in df.columns:
        if 'cumulative' in column.lower() or column.lower() in ['intakecount']:
            df[column] = df[column].fillna(method='ffill').fillna(0)
        else:
            df[column] = df[column].fillna(0)
    
    df.index = df.index.date

    if output:
        df.to_excel(output_file)

    return df

    
def died_and_survivors_to_xlsx(data, output_file):
    modified = {}
    
    for died in data[0]:
        modified[died['date']] = {'died': died['value']}
        
    for survivor in data[1]:
        if survivor['date'] not in modified:
            modified[survivor['date']] = {}
        
        modified[survivor['date']]['survivors'] = survivor['value']

    for moved in data[2]:
        if moved['date'] not in modified:
            modified[moved['date']] = {}

        modified[moved['date']]['moved'] = moved['value']
    
    df = pd.DataFrame.from_dict(modified, orient='index')
    df.index = pd.to_datetime(df.index)
    
    idx = pd.date_range(df.index.min(), df.index.max())
    df = df.reindex(idx)
    
    df = df.sort_index().fillna(method='ffill').fillna(0)
    
    df.index = df.index.date
    df.to_excel(output_file)
    
    
parser_mappings = {
#    'age-distribution': alt_distribution_to_xlsx,
    'age-distribution-status': alt_distribution_to_xlsx,
    'behandelduur-distribution': alt_distribution_to_xlsx,
#    'age-distribution-died': distribution_to_xlsx,
#    'age-distribution-died-and-survivors': distribution_to_xlsx,
    'ic-count': date_based_data_to_xlsx,
    'intake-count': date_based_data_to_xlsx,
    'intake-cumulative': date_based_data_to_xlsx,
    'new-intake': alt_date_based_data_to_xlsx,
    'died-and-survivors-cumulative': died_and_survivors_to_xlsx,
    'global': global_to_xlsx
}

name_mappings = {
    'new-intake': ['value', 'not-confirmed'],
    'age-distribution-status': ['patients_in_icu', 'patients_in_hospital', 'recovered_patients', 'deceased_patients'],
    'behandelduur-distribution': ['patients_in_hospital', 'patients_in_icu', 'recovered_patients', 'deceased_patients'],
}

index_labels = {
    'behandelduur-distribution': 'treatment_time_in_days'
}

resp = get(f'{stichting_nice_url}/js/covid-19.js')
done_urls = []

for line in resp.text.splitlines():
    if 'url' in line.lower():
        url = [x.strip() for x in line.split('\'')]
        if url in done_urls:
            print(f'Skipping double url: {url}')
            continue
        if len(url) > 2 and url[1] in expected_mappings:
            name = url[1].strip('/').split('/')[-1]
            print(f'Downloading {url[1]} to {name}.json')
            data_req = get(f'{stichting_nice_url}{url[1]}')
            done_urls.append(url)
            expected_mappings.remove(url[1])
            
            data = {'data': data_req.json()}
            
            data['license'] = stichting_nice_license
            data['source'] = data_req.url
            
            with open(data_output_path / f'{name}.json', 'w') as fh:
                fh.write(json.dumps(data, sort_keys=True, indent=4))
        
            if name in parser_mappings:
                if name in name_mappings:
                    if name in index_labels:
                        parser_mappings[name](data['data'], data_output_path / f'{name}.xlsx', name_mappings[name], index_labels[name])
                    else:
                        parser_mappings[name](data['data'], data_output_path / f'{name}.xlsx', name_mappings[name])
                else:
                    parser_mappings[name](data['data'], data_output_path / f'{name}.xlsx')
        else:
            print(f'Unknown url: {url}')
            
if len(expected_mappings) > 0:
    for mapping in expected_mappings:
        print(f'Missing the following dataset: {mapping}')

