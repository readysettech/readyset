CREATE TABLE notes (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    updated_at TIMESTAMP NOT NULL,
    created_by TEXT,
    title TEXT,
    body TEXT
);
