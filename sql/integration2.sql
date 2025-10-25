

--- tabla de estado del proceso de matching

-- Step 1: Create status table
CREATE TABLE IF NOT EXISTS match_process_status (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP DEFAULT now(),
    last_batch_at TIMESTAMP,
    total_processed INTEGER DEFAULT 0,
    total_matched INTEGER DEFAULT 0,
    total_unmatched INTEGER DEFAULT 0,
    completed BOOLEAN DEFAULT FALSE
);



--- procedimiento para procesar un batch de books

CREATE OR REPLACE PROCEDURE process_l1_l2_batch(status_id INT, batch_size INT DEFAULT 1000)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE temp_matched_rows ON COMMIT DROP AS
    SELECT
        NULL::REAL AS sim,
        NULL::BOOLEAN AS l1_correct,
        NULL::TEXT AS l1_title,
        NULL::TEXT AS l1_description,
        NULL::TEXT AS l1_authors,
        NULL::TEXT AS l1_publisher,
        NULL::DATE AS l1_publisheddate,
        NULL::TEXT AS l1_categories,
        NULL::TEXT AS l2_isbn,
        NULL::TEXT AS l2_book_title,
        NULL::TEXT AS l2_book_author,
        NULL::INT AS l1_id,
        NULL::INT AS l2_id
    LIMIT 0;

    INSERT INTO temp_matched_rows (
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    )
    SELECT 
        similarity(l1.title, l2.book_title) AS sim,
        l1.correct, l1.title, l1.description, l1.authors,
        l1.publisher, l1.publisheddate, l1.categories,
        l2.isbn, l2.book_title, l2.book_author,
        l1.id, l2.id
    FROM (
        SELECT * FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ) l2
    CROSS JOIN LATERAL (
        SELECT *
        FROM l1_books l1
        WHERE l1.matched = FALSE
          AND l1.title % l2.book_title
          AND l1.authors ILIKE '%' || l2.book_author || '%'
        ORDER BY similarity(l1.title, l2.book_title) DESC
        LIMIT 1
    ) l1
    WHERE similarity(l1.title, l2.book_title) > 0.9;

    INSERT INTO l1_l2_matches (
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    )
    SELECT 
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    FROM temp_matched_rows;

    /* INSERT INTO l1_l2_matches (
        sim, l2_isbn, l2_book_title, l2_book_author
    )
    SELECT 
        NULL, l2.isbn, l2.book_title, l2.book_author
    FROM (
        SELECT * FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ) l2
    WHERE NOT EXISTS (
        SELECT 1 FROM temp_matched_rows m WHERE m.l2_book_title = l2.book_title
    ); */

    UPDATE l2_books
    SET matched = TRUE
    WHERE id IN (
        SELECT l2_id FROM temp_matched_rows
    );

    UPDATE l1_books
    SET matched = TRUE
    WHERE id IN (
        SELECT l1_id FROM temp_matched_rows
    );

    UPDATE match_process_status
    SET
        last_batch_at = now(),
        total_processed = total_processed + batch_size,
        total_matched = total_matched + (SELECT COUNT(*) FROM temp_matched_rows),
        total_unmatched = total_unmatched + (
            SELECT COUNT(*) FROM (
                SELECT * FROM (
                    SELECT * FROM l2_books
                    WHERE matched = TRUE
                    ORDER BY book_title ASC
                    LIMIT batch_size
                ) l2
                WHERE NOT EXISTS (
                    SELECT 1 FROM temp_matched_rows m WHERE m.l2_book_title = l2.book_title
                )
            ) AS unmatched
        )
    WHERE id = status_id;
END;
$$;


---========= 
--wrapper para ejecutar el procedimiento en batches



-- Step 3: Procedure to run all batches
CREATE OR REPLACE PROCEDURE run_full_matching(batch_size INT DEFAULT 1000)
LANGUAGE plpgsql
AS $$
DECLARE
    remaining INT;
    status_id INT;
BEGIN
    INSERT INTO match_process_status DEFAULT VALUES RETURNING id INTO status_id;

    LOOP
        SELECT COUNT(*) INTO remaining FROM l2_books WHERE matched = FALSE;
        EXIT WHEN remaining = 0;

        CALL process_l1_l2_batch(status_id, batch_size);
    END LOOP;

    UPDATE match_process_status
    SET completed = TRUE, last_batch_at = now()
    WHERE id = status_id;

    RAISE NOTICE 'Matching complete. Status ID: %', status_id;
END;
$$;


--- better step 3


CREATE OR REPLACE PROCEDURE run_full_matching(batch_size INT DEFAULT 1000)
LANGUAGE plpgsql
AS $$
DECLARE
    remaining INT;
    status_id INT;
BEGIN
    -- Start a new status record
    INSERT INTO match_process_status DEFAULT VALUES RETURNING id INTO status_id;

    LOOP
        -- Check how many l2_books are left to process
        SELECT COUNT(*) INTO remaining FROM l2_books WHERE matched = FALSE;
        EXIT WHEN remaining = 0;

        -- Call the batch processor (each call is its own transaction if auto-commit is ON)
        CALL process_l1_l2_batch(status_id, batch_size);

        -- Optional: log progress
        RAISE NOTICE 'Processed a batch. Remaining: %', remaining;
    END LOOP;

    -- Final update to mark the process as completed
    UPDATE match_process_status
    SET completed = TRUE, last_batch_at = now()
    WHERE id = status_id;

    RAISE NOTICE 'Matching complete. Status ID: %', status_id;
END;
$$;


---=========

-- Step 4: Check status
-- SELECT * FROM match_process_status ORDER BY id DESC LIMIT 1;


---- vieja forma de hacerlo:
DO $$
DECLARE
    batch_size INT := 1000;
BEGIN
    -- 1️⃣ Crear tabla temporal para almacenar matches
    DROP TABLE IF EXISTS temp_matched_rows;
    CREATE TEMP TABLE temp_matched_rows AS
    SELECT
        NULL::REAL AS sim,
        NULL::BOOLEAN AS l1_correct,
        NULL::TEXT AS l1_title,
        NULL::TEXT AS l1_description,
        NULL::TEXT AS l1_authors,
        NULL::TEXT AS l1_publisher,
        NULL::DATE AS l1_publisheddate,
        NULL::TEXT AS l1_categories,
        NULL::TEXT AS l2_isbn,
        NULL::TEXT AS l2_book_title,
        NULL::TEXT AS l2_book_author,
        NULL::INT AS l1_id,
        NULL::INT AS l2_id
    LIMIT 0;

    -- 2️⃣ Insertar matches en la tabla temporal
    INSERT INTO temp_matched_rows (
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    )
    SELECT 
        similarity(l1.title, l2.book_title) AS sim,
        l1.correct, l1.title, l1.description, l1.authors,
        l1.publisher, l1.publisheddate, l1.categories,
        l2.isbn, l2.book_title, l2.book_author,
        l1.id, l2.id
    FROM (
        SELECT * FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ) l2
    CROSS JOIN LATERAL (
        SELECT *
        FROM l1_books l1
        WHERE l1.matched = FALSE
          AND l1.title % l2.book_title
          AND l1.authors ILIKE '%' || l2.book_author || '%'
        ORDER BY similarity(l1.title, l2.book_title) DESC
        LIMIT 1
    ) l1
    WHERE similarity(l1.title, l2.book_title) > 0.9;

    -- 3️⃣ Insertar matches en tabla final
    INSERT INTO l1_l2_matches (
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    )
    SELECT 
        sim, l1_correct, l1_title, l1_description, l1_authors,
        l1_publisher, l1_publisheddate, l1_categories,
        l2_isbn, l2_book_title, l2_book_author, l1_id, l2_id
    FROM temp_matched_rows;

    -- 4️⃣ Insertar no-matches
    INSERT INTO l1_l2_matches (
        sim, l2_isbn, l2_book_title, l2_book_author
    )
    SELECT 
        NULL, l2.isbn, l2.book_title, l2.book_author
    FROM (
        SELECT * FROM l2_books
        WHERE matched = FALSE
        ORDER BY book_title ASC
        LIMIT batch_size
    ) l2
    WHERE NOT EXISTS (
        SELECT 1 FROM temp_matched_rows m WHERE m.l2_book_title = l2.book_title
    );

    -- 5️⃣ Actualizar flags
    UPDATE l2_books
    SET matched = TRUE
    WHERE id IN (
        SELECT l2_id FROM temp_matched_rows
    );

    UPDATE l1_books
    SET matched = TRUE
    WHERE id IN (
        SELECT l1_id FROM temp_matched_rows
    );

    -- 6️⃣ Limpiar tabla temporal
    DROP TABLE temp_matched_rows;

    RAISE NOTICE 'Batch de % procesado.', batch_size;
END $$;