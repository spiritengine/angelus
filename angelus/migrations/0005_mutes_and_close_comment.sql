CREATE TABLE mutes (
    id INTEGER PRIMARY KEY,
    dedup_key TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    comment TEXT
);

CREATE INDEX idx_mutes_dedup_key
    ON mutes (dedup_key);

ALTER TABLE incidents
    ADD COLUMN close_comment TEXT;
