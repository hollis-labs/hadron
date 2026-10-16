-- Names this database's event log for resumable feeds. An event cursor carries
-- the generation it was issued under; a different generation means the log is
-- a different one (a new or replaced database), so the cursor cannot be
-- resumed. Additive, one row. Migrations here are forward-only; to roll back
-- run DROP TABLE store_generation (cursors issued with a generation then fail
-- as invalid; cursors issued without one keep working).
CREATE TABLE store_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation TEXT NOT NULL CHECK (length(generation) = 32)
);
INSERT INTO store_generation(id, generation) VALUES (1, lower(hex(randomblob(16))));
