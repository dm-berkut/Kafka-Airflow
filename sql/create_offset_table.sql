-- Cоздаём служебную таблицу. Она будет хранить информацию о том, до какого id producer уже дошёл.

CREATE TABLE IF NOT EXISTS kafka_dev.producer_offset_berkut (
    topic_name TEXT PRIMARY KEY,
    last_id BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
