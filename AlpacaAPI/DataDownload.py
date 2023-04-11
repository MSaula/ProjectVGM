import os
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from time import sleep

from pandas import Timestamp

from AlpacaAPI.AlpacaDataProvider import AlpacaDataProvider
from Utils.DateCursor import DateCursor


def download_data(data_provider: AlpacaDataProvider, final_sorting: bool = False) -> pd.DataFrame:
    all_instances: pd.DataFrame = pd.DataFrame()
    next_token: Optional[str] = None
    timer_reference: float = time.time()
    request_count: int = 0

    while True:
        next_token, raw_data = data_provider.execute_request(next_token)
        if raw_data is None: break

        data: pd.DataFrame = data_provider.generate_dataframe(raw_data)
        print("[%d] Fetched %d values between %s and %s from %s" % (
            request_count,
            data.shape[0],
            str(data.index.min()),
            str(data.index.max()),
            str(set(data['symbol'])) if data.get('symbol') is not None else '###'
        ))

        # Append the trades DataFrame to the all_trades DataFrame
        all_instances = pd.concat([all_instances, data])

        # If 200 requests have been made, sleep for 60 seconds to respect the rate limit
        request_count += 1
        if request_count % 200 == 0:
            time_elapsed = time.time() - timer_reference
            time_to_sleep = max(0.0, 60.0 - time_elapsed)

            if time_to_sleep > 0:
                print("Reached 200 requests quickly. Sleeping %.2fs" % time_to_sleep)
                sleep(time_to_sleep)

            timer_reference = time.time()

        if next_token is None: break

    print("Ended download...")
    if final_sorting:
        print("Performing final sorting...")
        all_instances.sort_index(inplace=True)
    return all_instances


def massive_download(
        data_type: str,
        symbol: str,
        start_date: str = '2000-01-01T00:00:00Z',
        end_date: str = '2023-04-02T23:59:59Z'
):
    data_provider = AlpacaDataProvider(symbol=[symbol], start_date=start_date, end_date=end_date, data_type=data_type)
    data = data_provider.get_first_instance(symbol)

    initial_date: Timestamp = data.index.min()

    cursor = DateCursor(
        start_date=initial_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_date=end_date,
        step={
            'weeks': 25
        },
        date_format="%Y-%m-%dT%H:%M:%SZ",
    )

    folder = f'{data_type}_{symbol}'
    os.makedirs(folder, exist_ok=True)

    batch_start = cursor.get()
    batch_end = cursor.next()
    while batch_end is not None:
        print(f"\nStarting batch of trades from {batch_start} to {batch_end}\n")

        ref = time.time()
        download_data(
            AlpacaDataProvider(
                symbol=[symbol], start_date=batch_start, end_date=batch_end,
                data_type=data_type, massive_download=True
            )
        ).to_csv(f"{folder}/{data_type[0]}_{cursor.get_formatted('%Y-%m')}.csv")

        elapsed_time = timedelta(seconds=(time.time() - ref))
        formatted_time = (datetime(1970, 1, 1) + elapsed_time).strftime("%Hh %Mm %Ss")
        print(f"\nDownload ended in {formatted_time}\n")

        batch_start = batch_end
        batch_end = cursor.next()


def download_top15_bars():
    download_data(
        AlpacaDataProvider(
            symbol=[
                'AAPL', 'MSFT', 'GOOG', 'AMZN', 'NVDA', 'TSLA', 'META',
                'XOM', 'JPM', 'CVX', 'BAC', 'KO', 'BABA', 'PFE'
            ],
            start_date='2000-01-03T00:00:00Z',
            end_date='2023-04-02T23:59:59Z',
            data_type='bars',
            timeframe='1Hour'
        )
    ).to_csv("bars_v1.csv")


def download_all_trades():
    download_data(
        AlpacaDataProvider(
            symbol=[
                'AAPL'
            ],
            # start_date='2000-01-01T00:00:00Z',
            start_date='2023-04-02T00:00:00Z',
            end_date='2023-04-02T23:59:59Z',
            data_type='trades'
        )
    ).to_csv("quotes_aapl.csv")


if __name__ == "__main__":
    massive_download(
        data_type='quotes',
        symbol='AAPL',
        start_date='2000-01-01T00:00:00Z',
        end_date='2023-04-02T23:59:59Z',
    )
