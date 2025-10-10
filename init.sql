DROP TABLE IF EXISTS l1_books;
DROP TABLE IF EXISTS l2_books;
DROP TABLE IF EXISTS l2_ratings;

CREATE TABLE l1_books (
    title TEXT,
    description TEXT,
    authors TEXT,
    publisher TEXT,
    publisheddate TEXT,
    categories TEXT
);

CREATE TABLE l2_books (
    isbn TEXT,
    book_title TEXT,
    book_author TEXT
);

CREATE TABLE l2_ratings (
    user_id TEXT,
    isbn TEXT,
    book_rating TEXT
);

-- Import data from CSVs
COPY l1_books FROM '/docker-entrypoint-initdb.d/L1-books.csv' DELIMITER ',' CSV HEADER;
COPY l2_books FROM '/docker-entrypoint-initdb.d/L2-books.csv' DELIMITER ',' CSV HEADER;
COPY l2_ratings FROM '/docker-entrypoint-initdb.d/L2-ratings.csv' DELIMITER ',' CSV HEADER;
