import pandas as pd
import numpy as np

import os
from sqlalchemy import select, case
from sqlalchemy import create_engine, MetaData, Table, select, func
from sqlalchemy.sql.elements import BooleanClauseList
from nltk.stem.snowball import SnowballStemmer 

stemmer = SnowballStemmer("russian") 
AIRFLOW_CONN_REVIEWS_DB = os.getenv('AIRFLOW_CONN_REVIEWS_DB')

def init_query(table_name):
    engine = create_engine(AIRFLOW_CONN_REVIEWS_DB)
    conn = engine.connect()
    metadata = MetaData(bind=engine)
    table = Table(table_name, metadata, autoload=True)
    return conn, table

def get_comments_from_table(Query_Handler):
    conn, reviews = init_query('reviews')
    query = select([reviews.c.id, reviews.c.content])\
            .where(Query_Handler.filter_)\
            .order_by(Query_Handler.order_)\
            .limit(Query_Handler.count_)
    Query_Handler.count_ += 100
    result = conn.execute(query)
    output = {}
    for row in result:
        output.update({row[0]: row[1]})
    return output

def get_sim_comments_from_table(index, Query_Handler):
    Query_Handler.filter_ = True
    conn, reviews = init_query('reviews')
	target_comment = select(reviews.c.embeddings)\
            .where(reviews.c.id == index).subquery()
    Query_Handler.order_ = reviews.c.embeddings.l2_distance(target_comment)
    query = select([reviews.c.id, reviews.c.content])\
            .order_by(Query_Handler.order_)\
            .limit(Query_Handler.count_)
    result = conn.execute(query)
    output = {}
    for row in result:
        output.update({row[0]: row[1]})
	return output

class Query_Handler():

    filter_ = True  # Current filter
    order_ = None   # Current order
    count_ = 100  # Current rows to show

    OPERATORS = {'ИЛИ': (1, lambda x, y: x | y), 
                 'НЕ': (3, lambda x: not_(x)),
                 'И': (2, lambda x, y: x & y)}

    @classmethod  
    def parse(cls, query):
        for s in query.split():
            if s in cls.OPERATORS:
                yield s 
            elif "(" in s:
                token = ''
                for i in s:
                    if "(" == i:
                        yield i
                    else:
                        token += i 
                if token:
                    yield  token.replace('"', '') if '"' in token else stemmer.stem(token)
            elif ")" in s:
                token = ''
                for i in s:
                    if ")" == i:
                        if token:
                            yield token.replace('"', '') if '"' in s else stemmer.stem(token)
                            token = ''
                        yield i
                    else:
                        token += i
            else:
                yield s.replace('"', '') if '"' in s else stemmer.stem(s)

    @classmethod              
    def shunting_yard(cls, parsed_formula):
        stack = []  # в качестве стэка используем список
        for token in parsed_formula:
            # если элемент - оператор, то отправляем дальше все операторы из стека, 
            # чей приоритет больше или равен пришедшему,
            # до открывающей скобки или опустошения стека.
            # здесь мы пользуемся тем, что все операторы право-ассоциативны
            if token in cls.OPERATORS: 
                while stack and stack[-1] != "(" and cls.OPERATORS[token][0] <= cls.OPERATORS[stack[-1]][0]:
                    yield stack.pop()
                stack.append(token)
            elif token == ")":
                # если элемент - закрывающая скобка, выдаём все элементы из стека, до открывающей скобки,
                # а открывающую скобку выкидываем из стека.
                while stack:
                    x = stack.pop()
                    if x == "(":
                        break
                    yield x
            elif token == "(":
                # если элемент - открывающая скобка, просто положим её в стек
                stack.append(token)
            else:
                # если элемент - число, отправим его сразу на выход
                yield token
        while stack:
            yield stack.pop()

    @classmethod    
    def calc(cls, tokens):
        stack = []
        for i in tokens:
            if i in cls.OPERATORS:
                if i == 'НЕ':
                    x = stack.pop() 
                    x = x if type(BooleanClauseList) else reviews.c.content.ilike(f'%{x}%')
                    stack.append(cls.OPERATORS[i][1](x))
                else:
                    y, x = stack.pop(), stack.pop()
                    y = y if type(y) == BooleanClauseList else reviews.c.content.ilike(f'%{y}%')
                    x = x if type(x) == BooleanClauseList else reviews.c.content.ilike(f'%{x}%')
                    stack.append(cls.OPERATORS[i][1](x, y))
            else:
                stack.append(i)
        return stack[0]

    @classmethod 
    def query(cls, q):
        # conn, rating = init_query('rating')
        # вернуть дефолтный порядок сортировки
        if len(q) > 0: 
            cls.filter_ = cls.calc(cls.shunting_yard(cls.parse(q)))
        else:
            cls.filter_ = True 

        cls.order_ = None
        cls.order_ = 100
        return filter_


