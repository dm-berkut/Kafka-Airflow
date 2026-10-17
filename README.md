# Проект: Kafka Producer, Kafka Consumer и Airflow

Проект реализует загрузку событий из PostgreSQL в Kafka, последующую обработку сообщений из Kafka и запись очищенных данных обратно в PostgreSQL. Запуск можно выполнять двумя способами: вручную через `spark-submit` или автоматически через DAG в Apache Airflow.

## Что делает проект

Источник данных - таблица `kafka_prod.events_source` в базе `kafka_task`.

Producer:

- читает из PostgreSQL новые записи, у которых `id` больше последнего обработанного;
- отправляет данные в Kafka topic `task_events_log_berkut`;
- обновляет служебную таблицу offset-ов `kafka_dev.producer_offset_berkut`.

Consumer:

- читает сообщения из Kafka topic `task_events_log_berkut`;
- очищает и валидирует поля `user_name`, `email`, `age`, `site_action`, `event_date`;
- записывает корректные данные в таблицу `kafka_dev.events_log_berkut`;
- использует checkpoint для хранения состояния Spark Structured Streaming.

## Структура проекта

```text
Kafka-Airflow/
├── dags/
│   ├── kafka_producer_berkut_dag.py
│   └── kafka_consumer_berkut_dag.py
├── scripts/
│   ├── producer_spark.py
│   └── consumer_spark.py
├── sql/
│   ├── create_offset_table.sql
│   └── create_target_table.sql
├── requirements.txt
├── setup.sh
└── README.md
```

## Используемые технологии

- Apache Spark / PySpark
- Spark Structured Streaming
- Apache Kafka
- PostgreSQL
- Apache Airflow

Для подключения Spark к Kafka и PostgreSQL используются пакеты:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2
org.postgresql:postgresql:42.7.5
```

## Подготовка PostgreSQL

Перед запуском нужно создать служебную таблицу offset-ов и целевую таблицу:

```bash
psql -h 172.17.1.76 -p 5432 -U lab_user -d kafka_task -f sql/create_offset_table.sql
psql -h 172.17.1.76 -p 5432 -U lab_user -d kafka_task -f sql/create_target_table.sql
```

Также должен существовать Kafka topic:

```text
task_events_log_berkut
```

## Локальный запуск

Проект можно запускать локально из корня репозитория.

Создать виртуальное окружение и установить Python-зависимости:

```bash
chmod +x setup.sh
./setup.sh
source .venv/bin/activate
```

Запустить producer:

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.5,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
  scripts/producer_spark.py
```

Запустить consumer:

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.5,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
  scripts/consumer_spark.py
```

Consumer создаст локальную папку `checkpoints/`. Она не должна загружаться в репозиторий, потому что содержит состояние Spark Streaming и Kafka offsets.

## Запуск на сервере через Airflow

На сервере DAG-файлы загружаются в папку:

```text
/opt/airflow/dags/d.berkut/
```

Spark-скрипты лежат в папке:

```text
/home/d.berkut/Kafka-Airflow/scripts/
```

В DAG указан рабочий каталог проекта:

```text
/home/d.berkut/Kafka-Airflow
```

Producer DAG:

- файл: `kafka_producer_berkut_dag.py`;
- `dag_id`: `kafka_producer_berkut`;
- расписание: каждый день в `08:00`;
- запускает `scripts/producer_spark.py`.

Consumer DAG:

- файл: `kafka_consumer_berkut_dag.py`;
- `dag_id`: `kafka_consumer_berkut`;
- расписание: каждый день в `19:00`;
- запускает `scripts/consumer_spark.py`.

Команда внутри DAG использует:

```text
/usr/local/spark/bin/spark-submit
```

Перед запуском DAG на сервере стоит проверить:

```bash
/usr/local/spark/bin/spark-submit --version
ls -la /home/d.berkut/Kafka-Airflow/scripts/
ls -la /opt/airflow/dags/d.berkut/
```

## Логика обработки данных

Поле `user_name`:

- удаляются лишние пробелы;
- значение приводится к формату с заглавной первой буквой каждого слова;
- пустые значения и строки длиной 100 символов и больше отбрасываются.

Поле `email`:

- приводится к нижнему регистру;
- проверяется регулярным выражением;
- некорректный email записывается как `NULL`.

Поле `age`:

- безопасно приводится к числу через `try_cast`;
- допустимый диапазон: от `1` до `100`;
- некорректные значения заменяются на `0`.

Поле `site_action`:

- приводится к нижнему регистру;
- допустимые значения: `login`, `logout`, `view`, `click`, `purchase`;
- записи с другими значениями не загружаются.

Поле `event_date`:

- приводится к типу timestamp;
- записи с некорректной датой не загружаются.

## Результат

После успешного запуска данные проходят цепочку:

```text
PostgreSQL kafka_prod.events_source
    -> Spark Producer
    -> Kafka topic task_events_log_berkut
    -> Spark Consumer
    -> PostgreSQL kafka_dev.events_log_berkut
```

Итоговая таблица содержит очищенные и провалидированные события пользователей.
