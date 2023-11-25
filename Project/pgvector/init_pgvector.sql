-- Install the extension we just compiled

CREATE EXTENSION IF NOT EXISTS vector;

CREATE ROLE postgres WITH LOGIN CREATEDB;
/*
For simplicity, we are directly adding the content into this table as
a column containing text data. It could easily be a foreign key pointing to
another table instead that has the content you want to vectorize for
semantic search, just storing here the vectorized content in our "items" table.

"768" dimensions for our vector embedding is critical - that is the
number of dimensions our transformer embeddings model output.
*/

CREATE TABLE reviews (id serial PRIMARY KEY,  
		      	content TEXT, 
			score INTEGER, 
			created_time TIMESTAMP, 
			country TEXT,
			source CHARACTER(10),
			sentiment CHARACTER(10),
			embeddings vector(100));

CREATE TABLE rating (score FLOAT,
					 source CHARACTER(10),
					 dates DATE,
					 count INTEGER
);

COPY reviews(content, score, created_time, country, source, sentiment, embeddings) FROM '/init_data/reviews.csv' DELIMITER '|' HEADER CSV;
COPY rating FROM '/init_data/rating.csv' DELIMITER ',' HEADER CSV;