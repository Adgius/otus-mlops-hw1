import os
from sqlalchemy import select, case
from sqlalchemy import create_engine, MetaData, Table, select, func
from pgvector.sqlalchemy import Vector


def get_avg_score_graph(date: str, Query_Handler):
    print('getting get_avg_score_graph...')
    conn, reviews = Query_Handler.conn_, Query_Handler.reviews_
    query = select([reviews.c.score, func.count(reviews.c.score)]).\
        where(reviews.c.created_time <= func.date(date)).group_by(reviews.c.score)
    result = conn.execute(query)
    res = {}
    for score, count in result:
        res.update({score: count})
    return res[1], res[2], res[3], res[4], res[5]
    
def get_avg_score(date, Query_Handler):
    print('getting get_avg_score...')
    # avg_score
    conn, reviews = Query_Handler.conn_, Query_Handler.reviews_
    query = select(func.avg(reviews.c.score))\
        .where(func.date(reviews.c.created_time) <= func.date(date))
    result = conn.execute(query)
    result = result.fetchone()
    avg_score = round(float(result[0]), 2)

    # avg_score_change
    today_cte = select([func.avg(reviews.c.score).label('t')])\
            .where(func.date(reviews.c.created_time) == func.date(date)).subquery()
    yesterday_cte = select([func.avg(reviews.c.score).label('y')])\
                    .where(func.date(reviews.c.created_time) == func.date(date) - 1).subquery()
    query = select(today_cte.c.t - yesterday_cte.c.y)
    result = conn.execute(query)
    result = result.fetchone()
    avg_score_change = float(result[0])

    # avg_score_change_sign
    avg_score_change_sign = '-' if avg_score_change < 0 else '+'

    avg_score_change = round(avg_score_change, 2)
    return avg_score, avg_score_change, avg_score_change_sign

def get_neg_score(date, Query_Handler):
    print('getting get_neg_score...')
    """
    with today as (
      select 
          avg(case when sentiment = 'NEGATIVE' then 1 else 0 end) as t
      from reviews r 
      where date(created_time) = current_date - 15
     ), 
    yesterday as (
      select 
          avg(case when sentiment = 'NEGATIVE' then 1 else 0 end) as y
      from reviews r 
      where date(created_time) = current_date - 16
     )
    select t - y
    from today
    cross join yesterday
    """
    # neg_score
    conn, reviews = Query_Handler.conn_, Query_Handler.reviews_
    case_expr = case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)
    query = select([func.avg(case_expr)])\
        .where(func.date(reviews.c.created_time) <= func.date(date))
    result = conn.execute(query)
    result = result.fetchone()
    neg_score = int(round(float(result[0]), 2) * 100)

    # neg_score_change
    today_cte = select([func.avg(case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)).label('t')]).\
            where(func.date(reviews.c.created_time) == func.date(date)).subquery()
    yesterday_cte = select([func.avg(case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)).label('y')]).\
                    where(func.date(reviews.c.created_time) == func.date(date) - 1).subquery()
    query = select([today_cte.c.t - yesterday_cte.c.y])
    result = conn.execute(query)
    result = result.fetchone()
    neg_score_change = float(result[0])
    neg_score_change_sign = '-' if neg_score_change < 0 else '+'
    neg_score_change = round(neg_score_change, 2) * 100
    return neg_score, neg_score_change, neg_score_change_sign

def get_rating_total(date, Query_Handler):
    print('get_rating_total...')
    conn, reviews = Query_Handler.conn_, Query_Handler.reviews_
    query = select([func.date(reviews.c.created_time).label('dt'), func.avg(reviews.c.score)]).\
            where((func.date(reviews.c.created_time) <= func.date(date)) & 
                  (func.date(reviews.c.created_time) > func.date(date) - 30)).\
        group_by(func.date(reviews.c.created_time)).\
        order_by(func.date(reviews.c.created_time))
    result = conn.execute(query)
    x, y = [], []
    for row in result:
        x.append(row[0].strftime('%Y-%m-%d'))
        y.append(round(float(row[1]), 2))
    return x, y

def get_neg_total(date, Query_Handler):
    print('getting get_neg_total...')
    conn, reviews = Query_Handler.conn_, Query_Handler.reviews_
    case_expr = case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)

    query = select([func.date(reviews.c.created_time).label('dt'),
                    func.avg(case_expr).label('neg')]).\
            where((func.date(reviews.c.created_time) <= func.date(date)) & 
                  (func.date(reviews.c.created_time) > func.date(date) - 30)).\
            group_by(func.date(reviews.c.created_time)).\
            order_by(func.date(reviews.c.created_time))
    result = conn.execute(query)
    x, y = [], []
    for row in result:
        x.append(row[0].strftime('%Y-%m-%d'))
        y.append(round(float(row[1]), 2) * 100)
    return x, y