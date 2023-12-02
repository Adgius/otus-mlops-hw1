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
    result = conn.execute(query, current_date_1=1)
    result = result.fetchone()
    avg_score_change = int(round(float(result[0]), 2))

    # avg_score_change_sign
    avg_score_change_sign = '-' if float(result[0]) < 0 else '+'

    return avg_score, avg_score_change, avg_score_change_sign

def get_neg_score():
    # neg_score
    conn, reviews = init_query('reviews')
    case_expr = case([(reviews.c.sentiment == 'NEGATIVE', 1)], else_=0)
    query = select([func.avg(case_expr)])
    values = {'sentiment_1': 'NEGATIVE',
          'param_1': 1,
          'param_2': 0}
    result = conn.execute(query, values)
    result = result.fetchone()
    neg_score = int(round(float(result[0]), 2))

    # neg_score_change
    neg_score_change = 10.5
    neg_score_change_sign = '-'
    return neg_score, neg_score_change, neg_score_change_sign

def get_rating_total():
    y = [4.47,4.79,4.64,4.66,4.56,4.57,4.51,4.59,4.45,4.36,4.73,4.49,4.52,4.56,4.4,4.51,4.45,4.57,4.54,4.55,4.58,4.55,4.65,4.63,4.57,4.77,4.66,4.62,4.51,4.54,4.48,4.54,4.74,4.78,4.78,4.62,4.57,4.68,4.47,4.56,4.54,4.67,4.59,4.6,4.58,4.78,4.52,4.52,4.55,4.65,4.52,4.62,4.46,4.65,4.66]
    return y

def get_neg_total():
    y = [4.47,4.79,4.64,4.66,4.56,4.57,4.51,4.59,4.45,4.36,4.73,4.49,4.52,4.56,4.4,4.51,4.45,4.57,4.54,4.55,4.58,4.55,4.65,4.63,4.57,4.77,4.66,4.62,4.51,4.54,4.48,4.54,4.74,4.78,4.78,4.62,4.57,4.68,4.47,4.56,4.54,4.67,4.59,4.6,4.58,4.78,4.52,4.52,4.55,4.65,4.52,4.62,4.46,4.65,4.66]
    return y