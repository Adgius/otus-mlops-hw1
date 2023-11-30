import pandas as pd
import numpy as np
from .manager import Data

from nltk.stem.snowball import SnowballStemmer 
stemmer = SnowballStemmer("russian") 


def get_comments_from_table(n=100, with_filter=False):
	if with_filter:
		return Data.data.loc[Data.similarity_order, 'content'][Data.current_filter][:n].to_dict()
	else:
		return Data.data.loc[Data.similarity_order, 'content'][:n].to_dict()

def get_sim_comments_from_table(index):
	cos_sim = lambda v, emb: np.dot(v, emb)/(np.linalg.norm(v)*np.linalg.norm(emb))
	sim = np.array(list(map(lambda x: cos_sim(Data.embeddings[index], x), np.array(list(Data.embeddings.values())))))
	Data.similarity_order = np.argsort(-sim)
	return Data.data.loc[Data.similarity_order, 'content'].to_dict()

class Query_Handler():

    OPERATORS = {'ИЛИ': (1, lambda x, y: x | y), 'НЕ': (3, lambda x: ~x),
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
	    cond_order = []
	    stack = []
	    for i in tokens:
		    if i in cls.OPERATORS:
		        cond_order.append(i)
		    else:
		    	cond_order.append(Data.data['content'].str.contains(i, regex=False, case=False).values)
	    for i in cond_order:
	    	if type(i) == np.ndarray:
	    		stack.append(i)
	    	else:
	    		if i == 'НЕ':
	    			el = stack.pop()
	    			stack.append(cls.OPERATORS[i][1](el))
	    		elif i == 'ИЛИ':
	    			el1, el2 = stack.pop(), stack.pop()
	    			stack.append(cls.OPERATORS[i][1](el1, el2))
	    		elif i == 'И':
	    			el1, el2 = stack.pop(), stack.pop()
	    			stack.append(cls.OPERATORS[i][1](el1, el2))
	    return stack[0]

    @classmethod 
    def query(cls, q):
        # вернуть дефолтный порядок сортировки
        Data.similarity_order = list(range(Data.data.shape[0]))
        if len(q) > 0: 
            q = Query_Handler.calc(Query_Handler.shunting_yard(Query_Handler.parse(q)))
            # поставить новый фильтр
            Data.current_filter = q
        else:
            Data.current_filter = np.ones(Data.current_filter.shape[0], dtype=bool)
        return get_comments_from_table(with_filter=True)


