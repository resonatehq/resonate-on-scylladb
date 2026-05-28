CREATE KEYSPACE IF NOT EXISTS resonate
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE resonate;

CREATE TABLE IF NOT EXISTS promises (
    id                 TEXT,
    origin             TEXT,
    branch             TEXT,
    parent             TEXT,
    target             TEXT,
    state              TEXT,
    param_headers      MAP<TEXT, TEXT>,
    param_data         TEXT,
    value_headers      MAP<TEXT, TEXT>,
    value_data         TEXT,
    tags               MAP<TEXT, TEXT>,
    timeout_at         BIGINT,
    created_at         BIGINT,
    settled_at         BIGINT,
    callbacks          SET<TEXT>,
    listeners          SET<TEXT>,
    task_state         TEXT,
    task_version       INT,
    task_ttl           BIGINT,
    task_pid           TEXT,
    task_resumes       SET<TEXT>,
    task_timeout_retry BIGINT,
    task_timeout_lease BIGINT,
    PRIMARY KEY (origin, id)
);

CREATE TABLE IF NOT EXISTS schedules (
    id                    TEXT,
    origin                TEXT,
    cron                  TEXT,
    promise_id            TEXT,
    promise_timeout       BIGINT,
    promise_param_headers MAP<TEXT, TEXT>,
    promise_param_data    TEXT,
    promise_tags          MAP<TEXT, TEXT>,
    next_run_at           BIGINT,
    last_run_at           BIGINT,
    created_at            BIGINT,
    create_token          UUID,
    PRIMARY KEY (origin, id)
);

CREATE TABLE IF NOT EXISTS promise_timeouts (
    bucket     BIGINT,
    shard      SMALLINT,
    timeout_at BIGINT,
    origin     TEXT,
    promise_id TEXT,
    PRIMARY KEY ((bucket, shard), timeout_at, origin, promise_id)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS task_timeouts (
    bucket             BIGINT,
    shard              SMALLINT,
    timeout_at         BIGINT,
    timeout_type       TINYINT,
    origin             TEXT,
    task_id            TEXT,
    promise_timeout_at BIGINT,
    PRIMARY KEY ((bucket, shard), timeout_at, timeout_type, origin, task_id)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS schedule_timeouts (
    bucket       BIGINT,
    shard        SMALLINT,
    timeout_at   BIGINT,
    origin       TEXT,
    schedule_id  TEXT,
    create_token UUID,
    PRIMARY KEY ((bucket, shard), timeout_at, origin, schedule_id, create_token)
) WITH CLUSTERING ORDER BY (timeout_at ASC);

CREATE TABLE IF NOT EXISTS workers (
    worker_id UUID,
    PRIMARY KEY (worker_id)
);
