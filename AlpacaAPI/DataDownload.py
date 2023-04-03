import time
from typing import Optional

import pandas as pd
from time import sleep

from AlpacaAPI.AlpacaDataProvider import AlpacaDataProvider


def download_data(data_provider: AlpacaDataProvider) -> pd.DataFrame:
    all_instances: pd.DataFrame = pd.DataFrame()
    next_token: Optional[str] = None
    timer_reference: float = time.time()
    request_count: int = 0

    while True:
        next_token, raw_data = data_provider.execute_request(next_token)
        if raw_data is None: break

        data: pd.DataFrame = data_provider.generate_dataframe(raw_data)
        print("Fetched %d values between %s and %s from %s" % (
            data.shape[0],
            str(data.index.min()),
            str(data.index.max()),
            str(set(data['symbol']))
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

    print("Ended download. Performing final sorting...")
    all_instances.sort_index(inplace=True)
    return all_instances


if __name__ == "__main__":
    """
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
    """
    download_data(
        AlpacaDataProvider(
            symbol=[
                'AAPL'
            ],
            start_date='2000-01-03T00:00:00Z',
            end_date='2023-04-02T23:59:59Z',
            data_type='trades'
        )
    ).to_csv("trades_aapl.csv")
