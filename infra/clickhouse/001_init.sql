CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8(
        'VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
        'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6
    ),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), timestamp, event_id);

CREATE TABLE IF NOT EXISTS cinema.movie_events_queue AS cinema.raw_events
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000,
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest_movie_events TO cinema.raw_events
AS SELECT
    event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM cinema.movie_events_queue;

CREATE TABLE IF NOT EXISTS cinema.daily_snapshots
(
    metric_date Date,
    processed_events UInt64,
    dau UInt64,
    average_watch_seconds Nullable(Float64),
    view_conversion Nullable(Float64),
    top_movies Array(Tuple(String, UInt64)),
    cohort_size UInt64,
    retention Array(Nullable(Float64)),
    computed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY metric_date;
