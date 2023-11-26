import pandas as pd
import datetime as dt
import torch
import fasttext
import requests
import os

from google_play_scraper import Sort, reviews
from datetime import timedelta, datetime
from itunes_app_scraper.scraper import AppStoreScraper
from app_store_scraper import AppStore

from transformers import AutoModelForSequenceClassification
from transformers import BertTokenizerFast

from tqdm import tqdm

from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.utils.sql import read_sql, load_from_pg, save_to_pg

tqdm.pandas()
pg_hook = PostgresHook(postgres_conn_id='postgres')

def scrap(date, n=3000):
    """
    end: datetime (monday)
    """
    country_lang_pairs = [('ru', 'ru'), ('ru', 'kz'), ('kz', 'kz'), ('ru', 'by'), ('by', 'by')]
    #GP
    gp = pd.DataFrame()
    for pair in country_lang_pairs:
        result, _ = reviews(
            'goldapple.ru.goldapple.customers',
            lang=pair[0],
            country=pair[1],
            sort=Sort.NEWEST, # defaults to Sort.NEWEST
            count=n, # defaults to 100
        )
        gp_ = pd.DataFrame(result)
        gp_ = gp_[['content', 'score', 'at']]
        gp_['country'] = pair[1]
        gp_['source'] = 'GooglePlay'
        assert gp_['at'].min() < date, 'Слишком мало отзывов в Google Play, укажите больший n'
        gp = pd.concat([gp, gp_])
        print(pair, gp.shape)

    #AS
    ap_s = pd.DataFrame()
    for country in ['ru', 'kz', 'by']:
        my_app = AppStore(
        country=country,
        app_name='Золотое Яблоко'.encode('utf-8'),
        app_id=1154436683
        )
        my_app.review(after=date, how_many=500)
        ap_s_ = pd.DataFrame(my_app.reviews)
        if ap_s_.shape[0] > 0:
            ap_s_ = ap_s_[['review', 'rating', 'date']]
            ap_s_['country'] = country
            ap_s_['source'] = 'AppStore'
            ap_s_.columns = ['content', 'score', 'at', 'country', 'source']
            ap_s = pd.concat([ap_s, ap_s_])

    result = pd.concat([gp, ap_s])
    result = result[result['at'].dt.date == date.date()]
    result.rename(columns={'at': 'created_time'}, inplace=True)
    return result.drop_duplicates('content')

def rating():
    result = app(
        'goldapple.ru.goldapple.customers',
        lang='ru', 
        country='ru' 

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
    res['dates'] = pd.to_datetime(date.date())
    return res

def predict(tokenizer, model, text):
    inputs = tokenizer(text, max_length=512, padding=True, truncation=True, return_tensors='pt')
    outputs = model(**inputs)
    predicted = torch.nn.functional.softmax(outputs.logits, dim=1)
    predicted = torch.argmax(predicted, dim=1).numpy()
    return predicted

def add_sentiment(df):
    tokenizer = BertTokenizerFast.from_pretrained('blanchefort/rubert-base-cased-sentiment')
    model = AutoModelForSequenceClassification.from_pretrained('blanchefort/rubert-base-cased-sentiment', return_dict=True)

    df['sentiment'] = df['content'].progress_apply(lambda x: predict(tokenizer, model, x)[0]) 
    df['sentiment'] = df['sentiment'].map({0: 'NEUTRAL', 1: 'POSITIVE', 2: 'NEGATIVE'})

    return df


def add_embedding(df):    
    stopwords = requests.get('https://raw.githubusercontent.com/negapedia/nltk/master/corpora/stopwords/russian').content.decode('utf-8').split('\n')
    text = load_from_pg(pg_hook, 'collect_content')
    text = text['content'].str.lower().str.replace(r'[^\w]', ' ', regex=True).str.replace(r'\s{2,}', ' ', regex=True)
    text = text.apply(lambda x: ' '.join([w  for w in x.split() if w not in stopwords]))
    with open('train.txt', 'w') as txt:
        txt.writelines(text)
    model = fasttext.train_unsupervised('train.txt', model='skipgram')
    df['embeddings'] = text.progress_apply(lambda x: model.get_sentence_vector(x))
    df['embeddings'] = df['embeddings'].apply(lambda x: x.tolist())
    save_to_pg(pg_hook, df, 'reviews')
    os.remove('train.txt')

def run(**kwargs):
    date = dt.datetime.strptime(kwargs['ds'], "%Y-%m-%d") - dt.timedelta(2)
    print(date)
    df = scrap(date)
    df = add_sentiment(df)
    add_embedding(df)