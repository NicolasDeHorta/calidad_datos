SELECT * FROM l1_books
-- title
-- description
-- authors (array)
-- publisher
-- publisheddate

SELECT * FROM l2_books
-- isbn
-- book_title
-- book_author

SELECT * FROM l2_ratings
-- user_id
-- isbn
-- book_rating 

SELECT DISTINCT 
    book_rating,
    CASE 
        WHEN book_rating ~ '^[0-9]+(\.[0-9]+)?$' THEN 'numeric'
        ELSE 'non-numeric'
    END AS type
FROM l2_ratings
ORDER BY type, book_rating;
-- tiene [null] a veces y despues numeros de 0 a 10 (12 distinct values)

WITH clean_ratings AS (
	SELECT *
	FROM l2_ratings 
	WHERE book_rating ~ '^[0-9]+(\.[0-9]+)?$'
)
SELECT 
	l2b.isbn,
	l2b.book_title,
	l2b.book_author,
	COUNT(l2r.book_rating::numeric) as count_rating,
	AVG(l2r.book_rating::numeric) AS avg_rating
FROM l2_books l2b
LEFT JOIN clean_ratings l2r
	ON l2r.isbn = l2b.isbn
GROUP BY l2b.isbn, l2b.book_title, l2b.book_author;



-- analisis de formatos de fechas 
SELECT DISTINCT TRANSLATE(publisheddate, '1234567890', 'xxxxxxxxxx')
FROM l1_books;


SELECT title, COUNT(*) AS cant
FROM l1_books
GROUP BY title
HAVING COUNT(*) > 1
ORDER BY cant DESC;

SELECT * FROM l1_books WHERE title = 'Microsoft&reg'


CREATE EXTENSION fuzzystrmatch;

SELECT difference('Anne', 'Ann')
