


--======= PASO Matcheamos por similaridad de titulos sin autores, dejamos sin matchear en book1 y book2
--guardamos la similaridad y los ids de ambos libros


-- Match and insert
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

--================ con estas consultas se verifica el universo
select count(*)
from l1_l2_matches AS m
inner join  l1_books as l1 ON l1.id = m.l1_id
where l1.matched=False
limit 100
--20770	true matched l1	 o2 l2
--40534 false matched l1 or l2
--total 61304 sin filtrar matched
-- sin tener en cuenta los autores

--================ PASO Matcheamos por similaridad de titulos y autores, dejamos sin matchear en book1 y book2
--guardamos la similaridad y los ids de ambos libros

-- primero seteamos fk de l1_l2_matches.l1_id y l2_id
ALTER TABLE l1_l2_matches
ADD CONSTRAINT fk_l1_books
FOREIGN KEY (l1_id) REFERENCES l1_books(id);

ALTER TABLE l1_l2_matches
ADD CONSTRAINT fk_l2_books
FOREIGN KEY (l2_id) REFERENCES l2_books(id);    

-- for each row in l1_l2_matches as m inner join l1_books as l1 on l1.id = m.l1_id where l1.matched is false, check if m.l1_authors is included in m.l2_book_author
UPDATE l1_l2_matches AS m
SET m.authors_match = TRUE
FROM l1_books AS l1
WHERE l1.id = m.l1_id
  AND l1.matched = FALSE
  AND m.l1_authors IS NOT NULL
  AND m.l2_book_author IS NOT NULL
  AND lower(trim(m.l2_book_author)) LIKE '%' || lower(trim(m.l1_authors)) || '%';

/*example data of non-matching authors:
"100 Poems by 100 Poets: An Anthology"	"100 Poems by 100 Poets: An Anthology"	"802132790"	"['John Carey']"	"Harold Pinter"
"100 Poems by 100 Poets: An Anthology"	"100 Poems by 100 Poets: An Anthology"	"802132790"	"['John Carey']"	"Harold Pinter"
*/

--create columns in l1_l2_matches to store if authors matched
ALTER TABLE l1_l2_matches
ADD COLUMN authors_match BOOLEAN;

-- set authors_match in l1_l2_matches to true when inner_join on id and l1_books.matched is true
update l1_l2_matches
set authors_match = true
from l1_books
where l1_l2_matches.l1_id = l1_books.id
  and l1_books.matched = true;
  --20770 rows updated

select count(*) 
from l1_l2_matches
where authors_match is null
--40534

select count(*) 
from l1_l2_matches as m
where authors_match is null
and m.l1_authors ILIKE '%' || m.l2_book_author || '%'
--948

select count(*) 
from l1_l2_matches as m
where authors_match is null
and similarity(m.l1_authors, m.l2_book_author) > 0.9
--1744

select count(*) 
from l1_l2_matches as m
where authors_match is null
and similarity(m.l1_authors, m.l2_book_author) > 0.9
and not( m.l1_authors ILIKE '%' || m.l2_book_author || '%')
--1018

select m.id, m.l1_title, m.l2_book_title, m.l2_isbn, m.l1_authors, m.l2_book_author
from l1_l2_matches as m
where authors_match is null
and similarity(m.l1_authors, m.l2_book_author) > 0.9
and not( m.l1_authors ILIKE '%' || m.l2_book_author || '%')
order by m.id
limit 100


SELECT 
  l1.title AS l1_title,
  l2.book_title AS l2_title,
  l1.authors AS l1_authors,
  l2.book_author AS l2_author,
  similarity(l1.authors, l2.book_author) AS sim_ac,
  similarity(l1.title, l2.book_title) AS sim_tc
FROM l2_books l2
CROSS JOIN LATERAL (
  SELECT l1.title, l1.authors
  FROM l1_books l1
  WHERE similarity(l1.title_clean, l2.title_clean) > 0.9
  	AND (l1.author_clean ILIKE '%' || l2.author_clean || '%' OR similarity(l1.author_clean, l2.author_clean) > 0.9 )
) l1
:

-- 
SELECT 
  l1.title AS l1_title,
  l2.book_title AS l2_title,
  l1.authors AS l1_authors,
  l2.book_author AS l2_author,
  similarity(l1.authors, l2.book_author) AS sim_ac,
  similarity(l1.title, l2.book_title) AS sim_tc
FROM l2_books l2
CROSS JOIN LATERAL (
  SELECT l1.title, l1.authors
  FROM l1_books l1
  WHERE similarity(l1.title_clean, l2.title_clean) > 0.9
  	AND (l1.author_clean ILIKE '%' || l2.author_clean || '%' OR similarity(l1.author_clean, l2.author_clean) < 0.9 )
) l1;

-- create authors clean in l1l2_matches
ALTER TABLE l1_l2_matches
ADD COLUMN l1_author_clean TEXT,
ADD COLUMN l2_author_clean TEXT;

-- update authors clean in l1l2_matches
UPDATE l1_l2_matches
SET l1_author_clean = LOWER(REGEXP_REPLACE(l1_authors, '[^a-zA-Z0-9]', '', 'g')),
    l2_author_clean = LOWER(REGEXP_REPLACE(l2_book_author, '[^a-zA-Z0-9]', '', 'g'));


-- update records that match the nex query
SELECT m.l1_title, m.l2_book_title, m.l1_authors,  m.l2_book_author,
	similarity(m.l1_author_clean, m.l2_author_clean) as sima
FROM l1_l2_matches AS m
WHERE (similarity(m.l1_author_clean, m.l2_author_clean) > 0.9
  	OR (m.l1_author_clean ILIKE '%' || m.l2_author_clean || '%'))
  AND m.authors_match is null

INSERT INTO l1_l2_matches (
    sim, l1_correct, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories,
    l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
)


UPDATE l1_l2_matches AS m
SET m.authors_match = TRUE
WHERE (m.l1_author_clean ILIKE '%' || m.l2_author_clean || '%')
  	OR (similarity(m.l1_author_clean, m.l2_author_clean) > 0.9)
  AND m.authors_match is null


-- el caso de que tenga mas de un actor y ya haya matcheado con alguna opcion, no va a encontrar las anteriores.
UPDATE l1_l2_matches
SET authors_match = true
WHERE ((l1_author_clean ILIKE '%' || l2_author_clean || '%')
OR (similarity(l1_author_clean, l2_author_clean) > 0.9))
AND authors_match is null

--insert into l1_l2_matches the l1_books that l1_id is not in l1_l2_matches.l1_id and l1_books.matched is false
WITH unmatched_l1 AS (
    SELECT l1.id AS l1_id,
           l1.title AS l1_title,
           l1.description AS l1_description,
           l1.authors AS l1_authors,
           l1.publisher AS l1_publisher,
           l1.publisheddate AS l1_publisheddate,
           l1.categories AS l1_categories
    FROM l1_books l1
    WHERE 
      AND l1.id NOT IN (SELECT l1_id FROM l1_l2_matches)
)
INSERT INTO l1_l2_matches (
    l1_correct, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories
) SELECT                                                                     
    NULL, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories
FROM unmatched_l1;    

WITH unmatched_l1 AS (
    SELECT l1.id AS l1_id,
           l1.title AS l1_title,
           l1.description AS l1_description,
           l1.authors AS l1_authors,
           l1.publisher AS l1_publisher,
           l1.publisheddate AS l1_publisheddate,
           l1.categories AS l1_categories
    FROM l1_books l1
    WHERE l1.id NOT IN (SELECT l1_id FROM l1_l2_matches)
)
INSERT INTO l1_l2_matches (
    l1_correct, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, l1_id
)
SELECT                                                                     
    NULL, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, l1.l1_id
FROM unmatched_l1;

INSERT INTO l1_l2_matches (
    l2_correct, l2_title, l2_description, l2_authors,
    l2_publisher, l2_publisheddate, l2_categories, l2_id
)
SELECT 		NULL,
			l2.title AS l2_title,
           l2.description AS l2_description,
           l2.authors AS l2_authors,
           l2.publisher AS l2_publisher,
           l2.publisheddate AS l2_publisheddate,
           l2.categories AS l2_categories,
		   l2.id AS l2_id
FROM l2_books l2
WHERE l2.id NOT IN (SELECT l2_id FROM l1_l2_matches)

INSERT INTO l1_l2_matches (
    l1_correct, l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, l1_id
)
SELECT 		NULL,
			l1.title AS l1_title,
           l1.description AS l1_description,
           l1.authors AS l1_authors,
           l1.publisher AS l1_publisher,
           l1.publisheddate AS l1_publisheddate,
           l1.categories AS l1_categories,
		   l1.id AS l1_id
FROM l1_books l1
WHERE l1.id NOT IN (SELECT l1_id FROM l1_l2_matches where l1_id is not null)
--184904 rows inserted

INSERT INTO l1_l2_matches (
    l2_book_title, l2_isbn, l2_book_author, l2_id
)
SELECT		l2.book_title AS l2_book_title,
           l2.isbn AS l2_isbn,
           l2.book_author AS l2_author,
           l2.id AS l2_id
FROM l2_books l2
WHERE l2.id NOT IN (SELECT l2_id FROM l1_l2_matches where l2_id is not null)
--233686 rows inserted


SELECT *
FROM l1_l2_matches
ORDER BY LENGTH(l2_isbn) DESC
LIMIT 1;



CREATE TABLE NL_Books (
    id SERIAL PRIMARY KEY,
    isbn VARCHAR(13),
    title TEXT,
    description TEXT,
    authors TEXT,
    publisher TEXT,
    publisheddate TEXT,
    categories TEXT,
    avg_rating NUMERIC
);

DROP TABLE IF EXISTS NL_Books;

--insert data from l1_l2_matches on NL_Books --where authors_match is true, set avg-rating the average rating from l2_ratings for the isbn
INSERT INTO NL_Books (
    isbn, title, description, authors,
    publisher, publisheddate, categories, avg_rating
)
SELECT
    l2_isbn, l2_book_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, (SELECT AVG(book_rating::NUMERIC) FROM l2_ratings WHERE isbn = l2_isbn) AS avg_rating
FROM l1_l2_matches
WHERE authors_match = true;


SELECT AVG(book_rating::NUMERIC) FROM l2_ratings WHERE isbn= '034545104X';

-- set index on l2_ratings.isbn
CREATE INDEX idx_l2_ratings_isbn ON l2_ratings(isbn);
-- analyze
ANALYZE l2_ratings;

--create table with average ratings per isbn
CREATE TABLE l2_ratings_avg AS
SELECT isbn, AVG(book_rating::NUMERIC) AS avg_rating
FROM l2_ratings
GROUP BY isbn;


INSERT INTO NL_Books (
    isbn, title, description, authors,
    publisher, publisheddate, categories, avg_rating
)
SELECT
    l2_isbn, l2_book_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, r.avg_rating
FROM l1_l2_matches LEFT JOIN l2_ratings_avg AS r ON l1_l2_matches.l2_isbn = r.isbn
WHERE authors_match = true;


SELECT count(*)
FROM l1_l2_matches 
WHERE authors_match = true;
--22818
SELECT count(*)
FROM l1_l2_matches JOIN l2_ratings_avg AS r ON l1_l2_matches.l2_isbn = r.isbn
WHERE authors_match = true;
--21913
SELECT count(*)
FROM l1_l2_matches LEFT JOIN l2_ratings_avg AS r ON l1_l2_matches.l2_isbn = r.isbn
WHERE authors_match = true;
--22818


SELECT isbn, title, avg_rating, authors, publisher, publisheddate, categories,  description
FROM NL_Books
WHERE avg_rating IS NOT NULL
ORDER BY avg_rating DESC
LIMIT 100;

SELECT *
FROM l1_l2_matches
WHERE l2_isbn = '345338219';

--- tenemos los matched, ahora hay que pasar los de L1 que estan en matched y con l2_id null
--- y los de L2 que estan en matched y con l1_id null a NL_Books
INSERT INTO NL_Books (
    isbn, title, description, authors,
    publisher, publisheddate, categories, avg_rating
)SELECT
    NULL,l1_title, l1_description, l1_authors,
    l1_publisher, l1_publisheddate, l1_categories, NULL
    FROM l1_l2_matches
WHERE l1_id IS NOT NULL AND l2_id IS NULL;
--184904 rows inserted

INSERT INTO NL_Books (
    isbn, title, description, authors,
    publisher, publisheddate, categories, avg_rating
)SELECT
    l2_isbn, l2_book_title, NULL, l2_book_author,
    NULL, NULL, NULL, r.avg_rating
FROM l1_l2_matches LEFT JOIN l2_ratings_avg AS r ON l1_l2_matches.l2_isbn = r.isbn
WHERE l2_id IS NOT NULL AND l1_id IS NULL;
--233686 rows inserted


-- add index to optimize join on isbn in l1_l2_matches
CREATE INDEX idx_l1_l2_matches_l2_isbn ON l1_l2_matches(l2_isbn); 

SELECT count(*) FROM l2_ratings_avg LEFT OUTER JOIN l1_l2_matches ON l2_ratings_avg.isbn = l1_l2_matches.l2_isbn
WHERE l1_l2_matches.l2_isbn IS NULL;

SELECT
    isbn, NULL, NULL, NULL,
    NULL, NULL, NULL, avg_rating
FROM l2_ratings_avg 
LEFT OUTER JOIN l1_l2_matches 
ON l2_ratings_avg.isbn = l1_l2_matches.l2_isbn
WHERE l1_l2_matches.l2_isbn IS NULL;


--verifico que no haya nada de l1_l2_matches 
SELECT
    isbn, NULL, NULL, NULL,
    NULL, NULL, NULL, avg_rating
FROM l2_ratings_avg 
LEFT OUTER JOIN l1_l2_matches 
ON l2_ratings_avg.isbn = l1_l2_matches.l2_isbn
WHERE l1_l2_matches.l2_isbn IS NULL;



-- TODO
--insertar en NL_Books los l2_ratings_avg que no esten en l1_l2_matches.l2_isbn
INSERT INTO NL_Books (
    isbn, title, description, authors,
    publisher, publisheddate, categories, avg_rating
)
SELECT
    isbn, NULL, NULL, NULL,
    NULL, NULL, NULL, avg_rating
FROM l2_ratings_avg 
LEFT OUTER JOIN l1_l2_matches 
ON l2_ratings_avg.isbn = l1_l2_matches.l2_isbn
WHERE l1_l2_matches.l2_isbn IS NULL;