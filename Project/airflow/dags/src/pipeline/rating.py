from google_play_scraper import app
import datetime as dt
from itunes_app_scraper.scraper import AppStoreScraper
import pandas as pd

from src.utils.sql import read_sql, load_from_pg, save_to_pg
from airflow.providers.postgres.hooks.postgres import PostgresHook


pg_hook = PostgresHook(postgres_conn_id='postgres')

def rating(date):
    result = app(
        'goldapple.ru.goldapple.customers',
        lang='ru', # defaults to 'en'
        country='ru' # defaults to 'us'

        )
    df2 = pd.Series({'score': result['score'], 'ratings': result['ratings'], 'source': 'GooglePlay'})

    def find_score(app_res):
        score = 0
        for s in app_res:
            score += app_res[s] * s
        return score / sum(app_res.values())

    scraper = AppStoreScraper()
    app_res = scraper.get_app_ratings(1154436683, countries='ru')#['trackCensoredName']

    df3 = pd.Series({'score': find_score(app_res), 'ratings': sum(app_res.values()), 'source': 'AppStore'})
    res = pd.concat([df2, df3], axis=1).T
    res['dates'] = pd.to_datetime(date)
    return res[['score', 'source', 'dates', 'ratings']]


def run_rating(**kwargs):
    date = dt.datetime.strptime(kwargs['ds'], "%Y-%m-%d")
    print('rating, date', date)
    df = rating(date)
    save_to_pg(pg_hook, df, 'rating')