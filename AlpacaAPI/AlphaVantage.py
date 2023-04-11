import pandas as pd
import requests
import json

api_key = 'ZD9Y1XM17G8ALJOQ'
# api_key = 'demo'


def request_other_data(params):
    url = 'https://www.alphavantage.co/query'
    r = requests.get(url, params=params)
    return r.json()


def request_fundamental_data(function='CASH_FLOW'):
    url = 'https://www.alphavantage.co/query'
    r = requests.get(
        url,
        params={
            'function': function,
            'symbol': 'IBM',
            'apikey': api_key
        })
    return r.json()


def store_csv(json_data: dict, name: str):
    cpi_df = pd.DataFrame(json_data['data'])
    cpi_df['date'] = pd.to_datetime(cpi_df['date'])
    cpi_df.set_index('date', inplace=True)
    cpi_df.to_csv(f'{name}.csv')


def download_and_store_cpi():
    cpi_data = request_other_data({
        'function': 'CPI',
        'interval': 'monthly',
        'apikey': api_key
    })
    print(json.dumps(cpi_data, indent=4))
    store_csv(cpi_data, 'cpi')


def download_treasury_yields(interval='daily'):
    df = None
    for maturity in ['3month', '2year', '5year', '7year', '10year', '30year']:
        print(f"Requesting ty for maturity {maturity}")
        data = request_other_data({
            'function': 'TREASURY_YIELD',
            'interval': daily,
            'maturity': maturity,
            'apikey': api_key
        })

        new_df = pd.DataFrame(data['data'])
        new_df[maturity] = new_df['value']
        new_df = new_df.drop(columns=['value'])

        new_df['date'] = pd.to_datetime(new_df['date'])
        new_df.set_index('date', inplace=True)

        if df is None:
            df = new_df
        else:
            df = pd.merge(df, new_df, left_index=True, right_index=True)

        print(df.shape)

    df.to_csv('treasury_yields.csv')


if __name__ == "__main__":
    data = request_other_data(params={
        'function': 'REAL_GDP',
        'interval': 'quarterly',
        'apikey': api_key
    })

    print(json.dumps(data, indent=4))
    store_csv(data, 'us_quarterly_gdp')
