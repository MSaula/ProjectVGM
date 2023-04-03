from time import sleep

import pandas as pd
import requests

from AlpacaAPI import api_key, api_secret_key, generic_url


class AlpacaDataProvider:
    symbols: list[str]
    start_date: str
    end_date: str
    data_type: str
    other_params: dict

    def __init__(self, symbol, start_date, end_date, data_type, **kwargs):
        self.symbols = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.data_type = data_type
        self.other_params = kwargs

    def get_request_params(self, page_token):
        if self.data_type == 'trades':
            return {
                'start': self.start_date,
                'end': self.end_date,
                'limit': 10000,
                'symbols': ",".join(self.symbols),
                'page_token': page_token if page_token is not None else ''
            }
        elif self.data_type == 'bars':
            return {
                'start': self.start_date,
                'end': self.end_date,
                'limit': 10000,
                'symbols': ",".join(self.symbols),
                'timeframe': self.other_params['timeframe'],
                'page_token': page_token if page_token is not None else ''
            }
        else:
            return None

    def execute_request(self, page_token, retries=None):
        try:
            response = requests.get(generic_url + self.data_type, headers={
                'APCA-API-KEY-ID': api_key,
                'APCA-API-SECRET-KEY': api_secret_key
            }, params=self.get_request_params(page_token))
        except ConnectionError:
            print("ConnectionError raised. Retrying download")
            return self.execute_request(page_token, retries=(retries - 1) if retries is not None else 5)

        if response.status_code == 200:
            data = response.json()
            return data['next_page_token'], data[self.data_type]

        else:
            if response.status_code == 400:
                print('Error 400. Bad Request. Ending program')
            elif response.status_code == 403:
                print('Error 403. Forbidden error. Ending program')
            elif response.status_code == 422:
                print('Error 422. Unprocessable (Invalid query parameter). Ending program')
                print(response.json())
            elif response.status_code == 429 and (retries is None or retries > 0):
                sleep(10)
                return self.execute_request(page_token, retries=(retries - 1) if retries is not None else 5)
            else:
                print("Unknown error. Code %d" % response.status_code)

            return None, None

    def generate_dataframe(self, data: dict) -> pd.DataFrame:
        df = pd.DataFrame()
        for symbol in self.symbols:
            if symbol not in data:
                continue

            new_df: pd.DataFrame = pd.DataFrame(data[symbol])
            new_df['symbol'] = symbol
            new_df['t'] = pd.to_datetime(new_df['t'])
            new_df.set_index('t', inplace=True)

            df = pd.concat([df, new_df])

        df.sort_index(inplace=True)

        if self.data_type == 'trades':
            if 'u' in df.columns:
                df = df[df['u'] == 'canceled']
                df = df.drop(columns=['u'])

            df = df.drop(columns=['c', 'i', 'z'])

        return df

