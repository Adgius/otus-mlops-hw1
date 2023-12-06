import numpy as np

x = ['2023-11-15', '2023-11-16', '2023-11-17', '2023-11-18',
               '2023-11-19', '2023-11-20', '2023-11-21', '2023-11-22',
               '2023-11-23', '2023-11-24', '2023-11-25', '2023-11-26',
               '2023-11-27', '2023-11-28', '2023-11-29', '2023-11-30',
               '2023-12-01', '2023-12-02', '2023-12-03', '2023-12-04',
               '2023-12-05', '2023-12-06']

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

