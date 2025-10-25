


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