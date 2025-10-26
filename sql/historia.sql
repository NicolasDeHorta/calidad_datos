-- DESCRIPCION DEL PROCESO DE INTEGRACION DE DATOS

-- al inciar el docker compose por primera vez se ejecuta init.sql
CREATE TABLE l1_books (
    correct BOOLEAN,
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

COPY l1_books FROM '/docker-entrypoint-initdb.d/L1-books-reviewed.csv' DELIMITER ',' CSV HEADER;
COPY l2_books FROM '/docker-entrypoint-initdb.d/L2-books.csv' DELIMITER ',' CSV HEADER;
COPY l2_ratings FROM '/docker-entrypoint-initdb.d/L2-ratings.csv' DELIMITER ',' CSV HEADER;

-- se crean indices e importan extensiones necesarias para fuzzy matching

CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

SET pg_trgm.similarity_threshold = 0.9;

CREATE INDEX IF NOT EXISTS idx_l1_books_title_trgm ON l1_books USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_l2_books_title_trgm ON l2_books USING gin (book_title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_l1_books_authors_text ON l1_books USING gin (authors gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_l1_authors_trgm ON l1_books USING gin (authors gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_l1_books_title_authors_trgm ON l1_books USING gin ((title || ' ' || authors) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_l1_books_title_clean_trgm ON l1_books USING gin (title_clean gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_l2_books_title_clean_trgm ON l2_books USING gin (title_clean gin_trgm_ops);

-- fast exact-match index on normalized title
CREATE INDEX IF NOT EXISTS idx_l1_title_clean_btree
  ON l1_books (title_clean);

CREATE INDEX IF NOT EXISTS idx_l2_title_clean_btree
  ON l2_books (title_clean);

-- se crean columnas auxiliares para limpieza de datos (title_clean, author_clean) para luego matchear mas facilmente
-- se agrega indices correspondientes
-- === L1 ===
ALTER TABLE l1_books ADD COLUMN title_clean TEXT;
UPDATE l1_books
SET title_clean = LOWER(REGEXP_REPLACE(title, '[^a-zA-Z0-9]', '', 'g'));

-- === L2 ===
ALTER TABLE l2_books ADD COLUMN title_clean TEXT;
UPDATE l2_books
SET title_clean = LOWER(REGEXP_REPLACE(book_title, '[^a-zA-Z0-9]', '', 'g'));

-- se crea columna matched en ambas tablas para marcar si un libro ya fue matcheado y sus indices correspondientes
ALTER TABLE l1_books ADD COLUMN IF NOT EXISTS matched BOOLEAN DEFAULT FALSE;
ALTER TABLE l2_books ADD COLUMN IF NOT EXISTS matched BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_l2_matched ON l2_books(matched);
CREATE INDEX idx_l1_matched ON l1_books(matched);

-- se crea la tabla de matches donde se guardan los resultados del matching e intermedios necesarios

CREATE TABLE l1_l2_matches (
    id SERIAL PRIMARY KEY,
    sim NUMERIC,
    -- Campos de L1
    l1_correct BOOLEAN,
    l1_title TEXT,
    l1_description TEXT,
    l1_authors TEXT,
    l1_publisher TEXT,
    l1_publisheddate TEXT,
    l1_categories TEXT,
    -- Campos de L2
    l2_isbn TEXT,
    l2_book_title TEXT,
    l2_book_author TEXT
);


-- se inserta en matches los libros que tienen titulo limpio exacto y autores de L2 incluidos en autores de L1 (faltó hacere clean de autores)
WITH
    -- 1 Match por título y autor
matched_rows AS (
    SELECT
	1 AS sim,
	l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
	l1.publisher, l1.publisheddate, l1.categories,
	l2.isbn, l2.book_title AS l2_title, l2.book_author, l1.id AS l1_id, l2.id AS l2_id
FROM l1_books l1 INNER JOIN l2_books l2 ON l1.title_clean = l2.title_clean
WHERE l1.authors ILIKE '%' || l2.book_author || '%'
)
-- 2 inserto los matches en la tabla final
INSERT INTO l1_l2_matches (
        sim,
        l1_correct, l1_title, l1_description, l1_authors, 
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    )
SELECT 
    sim,
    correct, l1_title, description, authors, publisher, publisheddate, categories,
    isbn, l2_title, book_author, l1.id, l2.id
FROM matched_rows

-- se crean id en l1_books y l2_books para luego referenciarlos desde l1_l2_matches
-- y se crean FK en l1_l2_matches hacia l1_books y l2_books
--create unique autogen key l1_books
ALTER TABLE l1_books ADD COLUMN id SERIAL PRIMARY KEY;
--create unique autogen key l2_books
ALTER TABLE l2_books ADD COLUMN id SERIAL PRIMARY KEY;

--add 2 columns to l1_l2_matches to store fk references y las generamos
ALTER TABLE l1_l2_matches ADD COLUMN l1_id INT;
ALTER TABLE l1_l2_matches ADD COLUMN l2_id INT;

ALTER TABLE l1_l2_matches
ADD CONSTRAINT fk_l1_books
FOREIGN KEY (l1_id) REFERENCES l1_books(id);

ALTER TABLE l1_l2_matches
ADD CONSTRAINT fk_l2_books
FOREIGN KEY (l2_id) REFERENCES l2_books(id);  

-- se insertan en l1_l2_matches los matches por similaridad de titulos limpios sin considerar los autores ni las filas ya matcheadas
WITH matched_rows AS (
    SELECT
        l1.id AS l1_id,
        l2.id AS l2_id,
        similarity(l1.title_clean, l2.title_clean) AS sim,
        l1.correct, l1.title AS l1_title, l1.description, l1.authors,
        l1.publisher, l1.publisheddate, l1.categories,
        l2.isbn, l2.book_title AS l2_title, l2.book_author
    FROM l1_books l1
    JOIN l2_books l2 ON l1.title_clean % l2.title_clean
    WHERE l1.matched = FALSE AND l2.matched = FALSE
      AND similarity(l1.title_clean, l2.title_clean) > 0.9
)
INSERT INTO l1_l2_matches (
    sim, l1_correct, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories,
    l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
)
SELECT
    sim, correct, l1_title, description, authors,
    publisher, publisheddate, categories,
    isbn, l2_title, book_author, l1_id, l2_id
FROM matched_rows;


-- el caso de que tenga mas de un actor y ya haya matcheado con alguna opcion, no va a encontrar las anteriores.
UPDATE l1_l2_matches
SET authors_match = true
WHERE ((l1_author_clean ILIKE '%' || l2_author_clean || '%')
OR (similarity(l1_author_clean, l2_author_clean) > 0.9))
AND authors_match is null


--se leen datos y revisa matchean la expresion regular en correct
--se cargan datos y se setea en correct true si la fila matchea la expresion regular

--se crea tabla de matches
--se crean indices para acelerar matching
--se hizo match de titulo clean exacto y autores incluidos y marca con matched true en l1 y l2
--se hizo match fuzzy por titulo clean sin autores incluidos y NO se marca con matched true en l1 y l2

--=== descripcion de columnas de procesamiento
-- correct: indica si la row del cvs matchea la expresion regular esperada
-- matched: indica si el libro ya fue matcheado (true/false) y no requiere mas procesamiento
-- title_clean: titulo limpio (sin mayusculas, sin caracteres especiales)
-- author_clean: autor limpio (sin mayusculas, sin caracteres especiales)
-- authors_match: indica si los autores matchean (true/false)
-- sim: similaridad del match (0 a 1)
-- l1_id: id del libro en l1_books fk
-- l2_id: id del libro en l2_books fk