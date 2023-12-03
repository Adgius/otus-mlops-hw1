import os
from sqlalchemy import select, case
from sqlalchemy import create_engine, MetaData, Table, select, func

AIRFLOW_CONN_REVIEWS_DB = os.getenv('AIRFLOW_CONN_REVIEWS_DB')

def init_query(table_name):
    engine = create_engine(AIRFLOW_CONN_REVIEWS_DB)
    conn = engine.connect()
    metadata = MetaData(bind=engine)
    table = Table(table_name, metadata, autoload=True)
    return conn, table


def get_avg_score_graph():
    conn, reviews = init_query('reviews')
    query = select([reviews.c.score, func.count(reviews.c.score)]).\
        group_by(reviews.c.score)
    result = conn.execute(query)
    conn.close()
    for score, count in result:
        res.update({score: count})
    return res[1], res[2], res[3], res[4], res[5]
    
def get_avg_score():
    # avg_score
    conn, reviews = init_query('reviews')
    query = select(func.avg(reviews.c.score))
    result = conn.execute(query)
    result = result.fetchone()
    avg_score = round(float(result[0]), 2)

    # avg_score_change
    today_cte = select([func.avg(reviews.c.score).label('t')])\
            .where(func.date(reviews.c.created_time) == func.CURRENT_DATE()).subquery()
    yesterday_cte = select([func.avg(reviews.c.score).label('y')])\
                    .where(func.date(reviews.c.created_time) == func.CURRENT_DATE() - 1).subquery()
    query = select(today_cte.c.t - yesterday_cte.c.y)
    result = conn.execute(query)
    result = result.fetchone()
    avg_score_change = round(float(result[0]), 2)

    # avg_score_change_sign
    avg_score_change_sign = '-' if avg_score_change < 0 else '+'

    return avg_score, avg_score_change, avg_score_change_sign

def get_neg_score():
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
    conn, reviews = init_query('reviews')
    case_expr = case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)
    query = select([func.avg(case_expr)])
    result = conn.execute(query)
    result = result.fetchone()
    neg_score = int(round(float(result[0]), 2))

    # neg_score_change
    today_cte = select([func.avg(case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)).label('t')]).\
            where(func.date(reviews.c.created_time) == func.CURRENT_DATE()).subquery()
    yesterday_cte = select([func.avg(case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)).label('y')]).\
                    where(func.date(reviews.c.created_time) == func.CURRENT_DATE() - 1).subquery()
    query = select([today_cte.c.t - yesterday_cte.c.y])
    result = conn.execute(query)
    result = result.fetchone()
    neg_score_change = round(float(result[0]), 2)
    neg_score_change_sign = '-' if neg_score_change < 0 else '+'
    return neg_score, neg_score_change, neg_score_change_sign

def get_rating_total():
    """
    select date(created_time) as dt, avg(score) from reviews r 
    group by dt
    order by dt
    limit 30
    """
    conn, reviews = init_query('reviews')
    query = select([func.date(reviews.c.created_time).label('dt'), func.avg(reviews.c.score)]).\
        group_by(func.date(reviews.c.created_time)).\
        order_by(func.date(reviews.c.created_time)).\
        limit(30)
    result = conn.execute(query)
    x, y = [], []
    for row in result:
        x.append(row[0].strftime('%Y-%m-%d'))
        y.append(round(float(row[1]), 2))
    return x, y

def get_neg_total():
    """
    select date(created_time) as dt, 
       avg(case when sentiment = 'NEGATIVE' then 1 else 0 end) as neg
    from reviews r
    group by dt
    order by dt
    limit 30
    """
    case_expr = case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)

    query = select([func.date(reviews.c.created_time).label('dt'),
                    func.avg(case_expr).label('neg')]).\
            group_by(func.date(reviews.c.created_time)).\
            order_by(func.date(reviews.c.created_time)).\
            limit(30)
    result = conn.execute(query)
    x, y = [], []
    for row in result:
        x.append(row[0].strftime('%Y-%m-%d'))
        y.append(round(float(row[1]), 2))
    return x, y