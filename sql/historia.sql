

--se crearon tablas
--se crean indices

--se leen datos y revisa matchean la expresion regular en correct
--se cargan datos y se setea en correct true si la fila matchea la expresion regular
--se crean columnas auxiliares para limpieza de datos (title_clean, author_clean) para luego matchear mas facilmente


--se crea tabla de matches
--se crean indices para acelerar matching
--se hizo match de titulo clean exacto y autores incluidos y marca con matched true en l1 y l2
--se hizo match fuzzy por titulo clean sin autores incluidos y NO se marca con matched true en l1 y l2

--=== descripcion de columnas de procesamiento
-- si ya matchea y no requiere
-- matched: indica si el libro ya fue matcheado (true/false)
-- title_clean: titulo limpio (sin mayusculas, sin caracteres especiales)
-- author_clean: autor limpio (sin mayusculas, sin caracteres especiales)
-- authors_match: indica si los autores matchean (true/false)
-- sim: similaridad del match (0 a 1)
-- l1_id: id del libro en l1_books fk
-- l2_id: id del libro en l2_books fk