import os
import numpy as np 
from sqlalchemy import select, case
from sqlalchemy import create_engine, MetaData, Table, select, func



def get_right_gp_score(date, Query_Handler):
    # right_gp_score
    conn, rating = Query_Handler.conn_, Query_Handler.rating_
    query = select(func.avg(rating.c.score)).\
        where((rating.c.dates <= func.date(date)) &
               (rating.c.source == 'GooglePlay'))
    result = conn.execute(query)
    result = result.fetchone()
    right_ga_score = round(float(result[0]), 2)

    # right_gp_score_change
    today_cte = select(func.avg(rating.c.score).label('t')).\
        where((rating.c.dates == func.date(date)) &
               (rating.c.source == 'GooglePlay')).subquery()

    yesterday_cte = select(func.avg(rating.c.score).label('y')).\
        where((rating.c.dates == func.date(date) - 1) &
               (rating.c.source == 'GooglePlay')).subquery()
    query = select([today_cte.c.t - yesterday_cte.c.y])
    result = conn.execute(query)
    result = result.fetchone()
    right_ga_score_change = float(result[0])
    right_ga_score_change_sign = '-' if right_ga_score_change < 0 else '+'
    right_ga_score_change = round(right_ga_score_change, 2)
    return right_ga_score, right_ga_score_change, right_ga_score_change_sign


def get_right_as_score(date, Query_Handler):
    # right_as_score
    conn, rating = Query_Handler.conn_, Query_Handler.rating_
    query = select(func.avg(rating.c.score)).\
        where((rating.c.dates <= func.date(date)) &
               (rating.c.source == 'AppStore'))
    result = conn.execute(query)
    result = result.fetchone()
    right_as_score = round(float(result[0]), 2)
    
    # right_as_score_change
    today_cte = select(func.avg(rating.c.score).label('t')).\
        where((rating.c.dates == func.date(date)) &
               (rating.c.source == 'AppStore')).subquery()

    yesterday_cte = select(func.avg(rating.c.score).label('y')).\
        where((rating.c.dates == func.date(date) - 1) &
               (rating.c.source == 'AppStore')).subquery()
    query = select([today_cte.c.t - yesterday_cte.c.y])
    result = conn.execute(query)
    result = result.fetchone()
    right_as_score_change = float(result[0])
    right_as_score_change_sign = '-' if right_as_score_change < 0 else '+'  
    right_as_score_change = round(right_as_score_change, 2)
    return right_as_score, right_as_score_change, right_as_score_change_sign

def get_right_ya_score():
    right_ya_score = np.random.randint(400, 500) / 100
    right_ya_score_change = np.random.randint(0, 20) / 100
    right_ya_score_change_sign = '+' if np.random.randint(0, 2) == 0 else '-'
    return right_ya_score, right_ya_score_change, right_ya_score_change_sign

def get_right_ga_score():
    right_ga_score = np.random.randint(400, 500) / 100
    right_ga_score_change = np.random.randint(0, 20) / 100
    right_ga_score_change_sign = '+' if np.random.randint(0, 2) == 0 else '-'
    return right_ga_score, right_ga_score_change, right_ga_score_change_sign