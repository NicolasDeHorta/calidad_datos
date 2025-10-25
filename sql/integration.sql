--NORMALIZACION E INDEXADO
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

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


VACUUM ANALYZE l1_books;
VACUUM ANALYZE l2_books;

-- === L1 ===
ALTER TABLE l1_books ADD COLUMN title_clean TEXT;
UPDATE l1_books
SET title_clean = LOWER(REGEXP_REPLACE(title, '[^a-zA-Z0-9]', '', 'g'));

-- === L2 ===
ALTER TABLE l2_books ADD COLUMN title_clean TEXT;
UPDATE l2_books
SET title_clean = LOWER(REGEXP_REPLACE(book_title, '[^a-zA-Z0-9]', '', 'g'));

ALTER TABLE l1_books ADD COLUMN IF NOT EXISTS matched BOOLEAN DEFAULT FALSE;
ALTER TABLE l2_books ADD COLUMN IF NOT EXISTS matched BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_l2_matched ON l2_books(matched);
CREATE INDEX idx_l1_matched ON l1_books(matched);


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

SET pg_trgm.similarity_threshold = 0.9;

SELECT 
  l1.title AS l1_title,
  l2.book_title AS l2_title,
  l1.authors AS l1_authors,
  l2.book_author AS l2_author,
  similarity(l1.title, l2.book_title) AS sim
FROM l2_books l2
CROSS JOIN LATERAL (
  SELECT l1.title, l1.authors
  FROM l1_books l1
  WHERE l1.title_clean = l2.book_title_clean
    AND l1.authors ILIKE '%' || l2.book_author || '%'
  ORDER BY similarity(l1.title_clean, l2.book_title_clean-) DESC
  LIMIT 1
) l1
ORDER BY sim DESC
LIMIT 50;


-- === JOIN de validación ===
SELECT COUNT(*) AS matches
FROM l1_books l1
INNER JOIN l2_books l2
  ON l1.title_clean = l2.title_clean;

SELECT * FROM l1_books  WHERE title = '1876' LIMIT 10


WITH l1 AS (
  SELECT * FROM l1_books
  ORDER BY title_clean ASC
  LIMIT 1000
),
l2 AS (
  SELECT * FROM l2_books
  ORDER BY title_clean ASC
  LIMIT 1000
)
SELECT 
  l1.title_clean AS l1_title,
  l2.title_clean AS l2_title,
  similarity(l1.title_clean, l2.title_clean) AS sim
FROM l1
JOIN l2
  ON l1.title % l2.title_clean
WHERE similarity(l1.title, l2.title_clean) > 0.9
ORDER BY sim ASC
LIMIT 100;




-- ===================================================
-- Ejecutar este bloque para procesar un batch
-- ===================================================

DO $$
DECLARE
    batch_size INT := 5000;
BEGIN
    WITH
    -- 1️⃣ Selecciono batch de L2 sin procesar
    l2_batch AS (
        SELECT *
        FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ),
    -- 2️⃣ Match fuzzy por título y autor
    matched_rows AS (
        SELECT 
            similarity(l1.title, l2.book_title) AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
            l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l2_batch l2
        CROSS JOIN LATERAL (
            SELECT *
            FROM l1_books l1
            WHERE l1.matched = FALSE
              AND l1.title % l2.book_title
              AND l1.authors ILIKE '%' || l2.book_author || '%'
            ORDER BY similarity(l1.title, l2.book_title) DESC
            LIMIT 1
        ) l1
        WHERE similarity(l1.title, l2.book_title) > 0.9
    ),
    -- 3️⃣ Inserto los matches en la tabla final
    inserted_matches AS (
        INSERT INTO l1_l2_matches (
            sim,
            l1_correct, l1_title, l1_description, l1_authors, 
            l1_publisher, l1_publisheddate, l1_categories,
            l2_isbn, l2_book_title, l2_book_author
        )
        SELECT 
            sim,
            correct, l1_title, description, authors, publisher, publisheddate, categories,
            isbn, l2_title, book_author
        FROM matched_rows
        RETURNING l1_title, l2_book_title
    )
    -- 4️⃣ Inserto los libros del batch sin match
    INSERT INTO l1_l2_matches (
        sim,
        l2_isbn, l2_book_title, l2_book_author
    )
    SELECT 
        NULL AS sim,
        l2.isbn, l2.book_title, l2.book_author
    FROM l2_batch l2
    WHERE NOT EXISTS (
        SELECT 1 FROM inserted_matches m WHERE m.l2_book_title = l2.book_title
    );

    -- 5️⃣ Actualizo flags en ambas tablas
    UPDATE l2_books
    SET matched = TRUE
    WHERE book_title IN (
        SELECT l2_book_title FROM l1_l2_matches
    );

    UPDATE l1_books
    SET matched = TRUE
    WHERE title IN (
        SELECT l1_title FROM l1_l2_matches WHERE l1_title IS NOT NULL
    );

    RAISE NOTICE 'Batch de % procesado.', batch_size;
END $$;



-- ===================================================
INSERT INTO l1_l2_matches (
    sim,
    l1_correct, l1_title, l1_description, l1_authors, 
    l1_publisher, l1_publisheddate, l1_categories,
    l2_isbn, l2_book_title, l2_book_author
) SELECT
            1 AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
            l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l1_books l1 INNER JOIN l2_books l2 ON l1.title_clean = l2.title_clean
        WHERE l1.authors ILIKE '%' || l2.book_author || '%'

DO $$
DECLARE
    batch_size INT := 5000;
BEGIN

    RAISE NOTICE 'Iniciando procesamiento de batch de %.', batch_size;
    matched_rows AS (
        SELECT 
            1 AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l1_books l1 INNER JOIN l2_books l2 ON l1.title_clean = l2.title_clean
        WHERE l1.authors ILIKE '%' || l2.book_author || '%'

        CROSS JOIN LATERAL (





















ECT 
            1 AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
            l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l2_batch l2
        CROSS JOIN LATERAL (
            SELECT *
            FROM l1_books l1
            WHERE l1.matched = FALSE
              AND l1.title = l2.book_title
              AND l1.authors ILIKE '%' || l2.book_author || '%'
            ORDER BY similarity(l1.title, l2.book_title) DESC
            LIMIT 1
        ) l1
    ),
    -- 3️⃣ Inserto los matches en la tabla final
    inserted_matches AS (
        INSERT INTO l1_l2_matches (
            sim,
            l1_correct, l1_title, l1_description, l1_authors, 
            l1_publisher, l1_publisheddate, l1_categories,
            l2_isbn, l2_book_title, l2_book_author
        )
        SELECT 
            sim,
            correct, l1_title, description, authors, publisher, publisheddate, categories,
            isbn, l2_title, book_author
        FROM matched_rows
        RETURNING l1_title, l2_book_title
    )
    -- 4️⃣ Inserto los libros del batch sin match
    INSERT INTO l1_l2_matches (
        sim,
        l2_isbn, l2_book_title, l2_book_author
    )
    SELECT 
        NULL AS sim,
        l2.isbn, l2.book_title, l2.book_author
    FROM l2_batch l2
    WHERE NOT EXISTS (
        SELECT 1 FROM inserted_matches m WHERE m.l2_book_title = l2.book_title
    );

    -- 5️⃣ Actualizo flags en ambas tablas
    UPDATE l2_books
    SET matched = TRUE
    WHERE book_title IN (
        SELECT l2_book_title FROM l1_l2_matches
    );

    UPDATE l1_books
    SET matched = TRUE
    WHERE title IN (
        SELECT l1_title FROM l1_l2_matches WHERE l1_title IS NOT NULL
    );

    RAISE NOTICE 'Batch de % procesado.', batch_size;
END $$;


-- ===================================================
matched_rows AS (
        SELECT 
            similarity(l1.title, l2.book_title) AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
            l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l2_batch l2
        CROSS JOIN LATERAL (
            SELECT *
            FROM l1_books l1
            WHERE l1.matched = FALSE
              AND l1.title % l2.book_title
              AND l1.authors ILIKE '%' || l2.book_author || '%'
            ORDER BY similarity(l1.title, l2.book_title) DESC
            LIMIT 1
        ) l1
        WHERE similarity(l1.title, l2.book_title) > 0.9
    ),




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
--RETURNING l1_title, l2_book_title

-- marco los filas de L2 como procesadas

    UPDATE l2_books
    SET matched = TRUE
    WHERE id IN (
       SELECT l2_id FROM matched_rows WHERE l2_title IS NOT NULL
    )

--marco las filas de L1 como procesadas

    UPDATE l1_books
    SET matched = TRUE
    WHERE id IN (
        SELECT l1_id FROM matched_rows WHERE l1_title IS NOT NULL
    )

-- 3 inserto los libros del batch sin match

-- empty l1_l2_matches table
TRUNCATE TABLE l1_l2_matches;

--create unique autogen key l1_books
ALTER TABLE l1_books ADD COLUMN id SERIAL PRIMARY KEY;
--create unique autogen key l2_books
ALTER TABLE l2_books ADD COLUMN id SERIAL PRIMARY KEY;
--add 2 columns to l1_l2_matches to store fk references
ALTER TABLE l1_l2_matches ADD COLUMN l1_id INT;
ALTER TABLE l1_l2_matches ADD COLUMN l2_id INT;


---==============================================

DO $$
DECLARE
    batch_size INT := 5000;
BEGIN
    WITH
    -- 1️⃣ Selecciono batch de L2 sin procesar
    l2_batch AS (
        SELECT *
        FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ),
    -- 2️⃣ Match fuzzy por título y autor
    matched_rows AS (
        SELECT 
            similarity(l1.title, l2.book_title) AS sim,
            l1.correct, l1.title AS l1_title, l1.description, l1.authors, 
            l1.publisher, l1.publisheddate, l1.categories,
            l2.isbn, l2.book_title AS l2_title, l2.book_author
        FROM l2_batch l2
        CROSS JOIN LATERAL (
            SELECT *
            FROM l1_books l1
            WHERE l1.matched = FALSE
              AND l1.title % l2.book_title
              AND l1.authors ILIKE '%' || l2.book_author || '%'
            ORDER BY similarity(l1.title, l2.book_title) DESC
            LIMIT 1
        ) l1
        WHERE similarity(l1.title, l2.book_title) > 0.9
    ),
    -- 3️⃣ Inserto los matches en la tabla final
    inserted_matches AS (
        INSERT INTO l1_l2_matches (
            sim,
            l1_correct, l1_title, l1_description, l1_authors, 
            l1_publisher, l1_publisheddate, l1_categories,
            l2_isbn, l2_book_title, l2_book_author
        )
        SELECT 
            sim,
            correct, l1_title, description, authors, publisher, publisheddate, categories,
            isbn, l2_title, book_author
        FROM matched_rows
        RETURNING l1_title, l2_book_title
    )
    -- 4️⃣ Inserto los libros del batch sin match
    INSERT INTO l1_l2_matches (
        sim,
        l2_isbn, l2_book_title, l2_book_author
    )
    SELECT 
        NULL AS sim,
        l2.isbn, l2.book_title, l2.book_author
    FROM l2_batch l2
    WHERE NOT EXISTS (
        SELECT 1 FROM inserted_matches m WHERE m.l2_book_title = l2.book_title
    );

    -- 5️⃣ Actualizo flags en ambas tablas
    UPDATE l2_books
    SET matched = TRUE
    WHERE book_title IN (
        SELECT l2_book_title FROM l1_l2_matches
    );

    UPDATE l1_books
    SET matched = TRUE
    WHERE title IN (
        SELECT l1_title FROM l1_l2_matches WHERE l1_title IS NOT NULL
    );

    RAISE NOTICE 'Batch de % procesado.', batch_size;
END $$;