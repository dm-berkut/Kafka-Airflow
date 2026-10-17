-- Запрос для создания итоговой таблицы

CREATE TABLE IF NOT EXISTS kafka_dev.events_log_berkut (
    id BIGINT NOT NULL,
    user_name VARCHAR(100),
    email VARCHAR(254),
    age SMALLINT,
    site_action VARCHAR(20),
    event_date TIMESTAMP,
    load_dttm TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS events_log_berkut_id_uidx
    ON kafka_dev.events_log_berkut (id);
