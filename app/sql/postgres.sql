CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    dimension TEXT NOT NULL DEFAULT '',
    value DOUBLE PRECISION,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE TABLE IF NOT EXISTS daily_exports (
    metric_date DATE PRIMARY KEY,
    exported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
