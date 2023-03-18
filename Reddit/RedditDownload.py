import csv
import time
from datetime import datetime, timedelta

import requests


def fetch_comments(base_url, stock_aliases, start_date, end_date, batch_size, delta):
    all_comments = []

    current_start_date = start_date
    current_end_date = current_start_date - delta

    while current_end_date >= end_date:
        results_buffer = {}
        for word in stock_aliases:
            ref = time.time()

            print(" -> New batch. From previous %d to %d previous days. Looking for word %s (n: %d / %d)" %
                  (current_start_date, current_end_date, word, len(all_comments), len(results_buffer)))

            params = {
                'q': word,
                'after': str(current_start_date) + 'd',
                'before': str(current_end_date) + 'd',
                'size': batch_size,
                'sort_type': 'score',
                'sort': 'score'
            }
            response = requests.get(base_url, params=params)

            if response.status_code == 200:
                data = response.json()

                # Se van recogiendo los valores en un diccionario para evitar duplicados
                for value in data['data']:
                    results_buffer[value['id']] = value
            else:
                print(f"Error {response.status_code} occurred while fetching comments.")

            time.sleep(max(.0, 1.0 - (time.time() - ref)))

        results = list(results_buffer.values())
        results.sort(key=lambda x: -x['score'])
        all_comments += results[:batch_size]

        current_start_date -= delta
        current_end_date -= delta

    # Sort the results in ascending order by the 'created_utc' field
    all_comments.sort(key=lambda x: x['created_utc'])
    return all_comments


def unix_to_date(unix_timestamp):
    return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')


def write_comments_to_csv(comments, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['timestamp', 'text', 'score', 'upvotes', 'downvotes', 'username', 'num_comments', 'subreddit']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for comment in comments:
            writer.writerow({
                'timestamp': unix_to_date(comment['created_utc']),
                'text': comment['body'],
                'score': comment['score'],
                'upvotes': comment.get('ups', None),  # Pushshift may not provide the upvotes and downvotes data
                'downvotes': comment.get('downs', None),
                'username': comment['author'],
                'subreddit': comment['subreddit']
            })


def write_posts_to_csv(comments, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'timestamp', 'title', 'score', 'upvotes', 'downvotes',
            'username', 'subreddit', 'num_comments', 'distinguished'
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for comment in comments:
            writer.writerow({
                'timestamp': unix_to_date(comment['created_utc']),
                'title': comment['title'],
                # 'text': comment['body'],
                'score': comment.get('score', None),
                'upvotes': comment.get('ups', None),
                'downvotes': comment.get('downs', None),
                'username': comment.get('author', None),
                'subreddit': comment.get('subreddit', None),
                'num_comments': comment.get('num_comments', None),
                'distinguished': comment.get('distinguished', None),
            })


def main():
    stocks = {
        'AAPL': ['AAPL', 'Tim Cook'],
        'MSFT': ['MSFT', 'Microsoft', 'Satya Nadella'],
        'AMZN': ['AMZN', 'Amazon', 'Jeff Bezos'],
        'SPY': ['SPY', 'S&P 500'],
        'GOOG': ['GOOG', 'Google', 'Alphabet inc.', 'Sundar Pichai'],
        'TSLA': ['TSLA', 'Tesla', 'Elon Musk'],
        'BBRK': ['BBRK', 'Berkshire Hathaway'],
        'JPM': ['JPM', 'JPMorgan', 'Jamie Dimon'],
        'CVX': ['CVX', 'Chevron', 'Mike Wirth'],
        'T': ['AT&T', 'John T. Stankey', 'John Stankey'],
    }

    params = {
        'start_date': 4000,
        'end_date': 1,
        'batch_size': 300,
        'delta': 28
    }

    for stock, aliases in stocks.items():
        print("#" * 75)

        print("Fetching %s comments" % stock)
        comments = fetch_comments(
            base_url="https://api.pushshift.io/reddit/search/comment/",
            stock_aliases=aliases,
            **params
        )
        print("Writing %d %s comments" % (len(comments), stock))
        write_comments_to_csv(comments, f"{stock}_comments.csv")

        print("Fetching %s submissions" % stock)
        submissions = fetch_comments(
            base_url="https://api.pushshift.io/reddit/search/submission/",
            stock_aliases=aliases,
            **params
        )
        print("Writing %d %s comments" % (len(submissions), stock))
        write_posts_to_csv(submissions, f"{stock}_submissions.csv")


if __name__ == "__main__":
    main()
