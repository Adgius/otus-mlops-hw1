-- Install the extension we just compiled

CREATE EXTENSION IF NOT EXISTS vector;

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
			at DATETIME, 
			country TEXT,
			source TEXT,
			embedding vector(768));
