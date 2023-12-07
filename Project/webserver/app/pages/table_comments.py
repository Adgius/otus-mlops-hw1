import os
from sqlalchemy import select, case
from sqlalchemy import create_engine, MetaData, Table, select, func
from sqlalchemy.sql.elements import BooleanClauseList
from nltk.stem.snowball import SnowballStemmer 
from pgvector.sqlalchemy import Vector

stemmer = SnowballStemmer("russian") 
AIRFLOW_CONN_REVIEWS_DB = os.getenv('AIRFLOW_CONN_REVIEWS_DB')
print(os.getenv('AIRFLOW_CONN_REVIEWS_DB'))

def init_query():
    engine = create_engine(AIRFLOW_CONN_REVIEWS_DB)
    conn = engine.connect()
    metadata = MetaData(bind=engine)
    reviews = Table('reviews', metadata, autoload=True)
    rating = Table('rating', metadata, autoload=True)
    return conn, reviews, rating


class Query_Handler():

    filter_ = True  # Current filter
    order_ = None   # Current order
    count_ = 100  # Current rows to show
    conn_, reviews_, rating_ = init_query()
    OPERATORS = {'ИЛИ': (1, lambda x, y: x | y), 
                 'НЕ': (3, lambda x: not_(x)),
                 'И': (2, lambda x, y: x & y)}

    @classmethod  
    def _parse(cls, query):
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
    def _shunting_yard(cls, parsed_formula):
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
    def _calc(cls, tokens):
        stack = []
        for i in tokens:
            if i in cls.OPERATORS:
                if i == 'НЕ':
                    x = stack.pop() 
                    stack.append(cls.OPERATORS[i][1](x))
                else:
                    y, x = stack.pop(), stack.pop()
                    stack.append(cls.OPERATORS[i][1](x, y))
            else:
                stack.append(cls.reviews_.c.content.ilike(f'%{i}%'))
        return stack[0]

    @classmethod 
    def query(cls, q):
        if len(q) > 0: 
            cls.filter_ = cls._calc(cls._shunting_yard(cls._parse(q)))
        else:
            cls.filter_ = True 

        cls.order_ = None
        cls.count_ = 100

    @classmethod 
    def get_comments_from_table(cls):
        query = select([cls.reviews_.c.id, cls.reviews_.c.content])\
                .where(cls.filter_)\
                .order_by(cls.order_)\
                .limit(cls.count_)
        cls.count_ += 100
        result = cls.conn_.execute(query)
        output = {}
        for row in result:
            output.update({row[0]: row[1]})
        return output

    @classmethod 
    def get_sim_comments_from_table(cls, index):
        cls.filter_ = True
        cls.count_ = 100
        target_comment = select(cls.reviews_.c.embeddings)\
                .where(cls.reviews_.c.id == index).subquery()
        cls.order_ = cls.reviews_.c.embeddings.l2_distance(target_comment)
        query = select([cls.reviews_.c.id, cls.reviews_.c.content])\
                .order_by(cls.order_)\
                .limit(cls.count_)
        result = cls.conn_.execute(query)
        output = {}
        for row in result:
            output.update({row[0]: row[1]})
        return output

