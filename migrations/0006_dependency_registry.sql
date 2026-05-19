-- Slice 5c: dependency registry.
--
-- 0001 created an early dep_health table (columns: dep, last_check_at,
-- status, details, updated_at) with no reader and no writer -- dead
-- scaffolding. Slice 5c gives dep health a real writer (the dep_record
-- control op) and a real reader (the health op), and pins the shape:
-- dependency_name PK, a status CHECK, last_check_at/updated_at NOT NULL,
-- a nullable detail. The old table was never written, so there is no
-- data to preserve; drop it and recreate with the pinned schema. Plain
-- statements only -- the migration runner wraps the file in its own
-- BEGIN/COMMIT and executes one statement at a time.
DROP TABLE dep_health;

CREATE TABLE dep_health (
    dependency_name TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('healthy', 'unhealthy')),
    last_check_at TEXT NOT NULL,
    detail TEXT,
    updated_at TEXT NOT NULL
);
