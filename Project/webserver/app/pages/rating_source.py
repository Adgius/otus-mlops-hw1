import numpy as np
from .manager import Data

x = Data.dates

def random_generate(min_, max_, n):
	x = np.random.rand(n)
	return  list(x * (max_ - min_) + min_)

def get_gp_score():
    y = random_generate(3.5, 5, len(x))
    return y

def get_gp_count():
    y = random_generate(5, 50, len(x))
    return y

def get_as_score():
    y = random_generate(4.5, 5, len(x))
    return y

def get_as_count():
    y = random_generate(5, 50, len(x))
    return y

def get_ya_score():
    y = random_generate(1, 5, len(x))
    return y

def get_ya_count():
    y = random_generate(1, 10, len(x))
    return y

def get_ga_score():
    y = random_generate(4, 5, len(x))
    return y

def get_ga_count():
    y = random_generate(1000, 5000, len(x))
    return y

