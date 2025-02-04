CREATE TABLE nginx_logs (
    id SERIAL PRIMARY KEY,
    time_local TIMESTAMP,
    remote_addr TEXT,
    request TEXT,
    status INT,
    body_bytes_sent INT,
    http_referer TEXT,
    http_user_agent TEXT
);