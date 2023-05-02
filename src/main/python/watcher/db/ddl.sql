CREATE TABLE events
(
  guid char(36) PRIMARY KEY NOT NULL,
  extract_date date NOT NULL,
  file_name char(255) UNIQUE NOT NULL,
  received_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  pushed_at datetime DEFAULT NULL
);