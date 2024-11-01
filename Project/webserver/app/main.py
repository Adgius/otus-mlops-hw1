import os
import datetime as dt
import json
import sys

from fastapi import FastAPI, Request, Body
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

# Удалить
from dotenv import load_dotenv
load_dotenv()
###########

from pages.insights import *
from pages.right_side import *
from pages.rating_source import *
from pages.table_comments import Query_Handler




templates = Jinja2Templates(directory="templates")

app = FastAPI(title='Reviews dashboard')

app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.getcwd(), "static")),
    name="static",
)
print(os.path.join(os.getcwd(), "static"))



@app.get('/show_more_comments')
def get_table_comments(**kwargs):
    comments = Query_Handler.get_comments_from_table()
    return comments

@app.get('/get_sim_comments_from_table')
def get_sim_table_comments(index: int):
    comments = Query_Handler.get_sim_comments_from_table(index)
    return comments  # need to avoid sorting by key

@app.get('/execute_query')
def execute_query(q: str):
    Query_Handler.query(q)
    comments = Query_Handler.get_comments_from_table()
    return comments

@app.post('/get_date')
def get_date(date = Body()):
    return date['date']

@app.get("/")
def get_base_page(request: Request, date: str = '2023-11-15'): # dt.datetime.now().strftime('%Y-%m-%d')
    rating_total_x, rating_total_y = get_rating_total(date, Query_Handler)
    neg_total_x, neg_total_y = get_neg_total(date, Query_Handler)
    avg_score_count_1, avg_score_count_2, avg_score_count_3, avg_score_count_4, avg_score_count_5 = get_avg_score_graph(date, Query_Handler)
    avg_score, avg_score_change, avg_score_change_sign = get_avg_score(date, Query_Handler)
    neg_score, neg_score_change, neg_score_change_sign = get_neg_score(date, Query_Handler)

    right_gp_score, right_gp_score_change, right_gp_score_change_sign = get_right_gp_score(date, Query_Handler)
    right_as_score, right_as_score_change, right_as_score_change_sign = get_right_as_score(date, Query_Handler)
    right_ya_score, right_ya_score_change, right_ya_score_change_sign = get_right_ya_score()
    right_ga_score, right_ga_score_change, right_ga_score_change_sign = get_right_ga_score()

    gp_score = get_gp_score()
    gp_count = get_gp_count()
    as_score = get_as_score()
    as_count = get_as_count()
    ya_score = get_ya_score()
    ya_count = get_ya_count()
    ga_score = get_ga_score()
    ga_count = get_ga_count()

    comments = Query_Handler.get_comments_from_table()

    params = {
              'date': date,
              'x': x, # from rating_source (test var)
              'request': request, 
              'rating_total_x': rating_total_x, 
              'rating_total_y': rating_total_y, 
              'neg_total_x': neg_total_x,
              'neg_total_y': neg_total_y, 
              'avg_score_count_1': avg_score_count_1,
              'avg_score_count_2': avg_score_count_2,
              'avg_score_count_3': avg_score_count_3,
              'avg_score_count_4': avg_score_count_4,
              'avg_score_count_5': avg_score_count_5,
              'avg_score': avg_score, 
              'avg_score_change': avg_score_change, 
              'avg_score_change_sign': avg_score_change_sign,
              'neg_score': neg_score, 
              'neg_score_change': neg_score_change, 
              'neg_score_change_sign': neg_score_change_sign,
              'right_gp_score': right_gp_score, 
              'right_gp_score_change': right_gp_score_change, 
              'right_gp_score_change_sign': right_gp_score_change_sign,
              'right_as_score': right_as_score, 
              'right_as_score_change': right_as_score_change, 
              'right_as_score_change_sign': right_as_score_change_sign,
              'right_ya_score': right_ya_score, 
              'right_ya_score_change': right_ya_score_change, 
              'right_ya_score_change_sign': right_ya_score_change_sign,
              'right_ga_score': right_ga_score, 
              'right_ga_score_change': right_ga_score_change, 
              'right_ga_score_change_sign': right_ga_score_change_sign,
              'gp_score': gp_score,
              'gp_count': gp_count,
              'as_score': as_score,
              'as_count': as_count,
              'ya_score': ya_score,
              'ya_count': ya_count,
              'ga_score': ga_score,
              'ga_count': ga_count,
              'comments': comments
              }
    return templates.TemplateResponse("index.html", params)