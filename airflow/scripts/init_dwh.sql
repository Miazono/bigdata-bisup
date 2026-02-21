CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS stg.clients (
    id              TEXT,
    name            TEXT,
    website         TEXT,
    industry        TEXT,
    manager         TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.campaigns (
    id              TEXT,
    client_id       TEXT,
    platform        TEXT,
    type            TEXT,
    start_date      TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.ad_stats (
    date            TEXT,
    campaign_id     TEXT,
    impressions     TEXT,
    clicks          TEXT,
    cost            TEXT,
    conversions     TEXT,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.site_monitoring (
    date                TEXT,
    client_id           TEXT,
    load_time_ms        TEXT,
    uptime_pct          TEXT,
    server_errors       TEXT,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.clients (
    id              VARCHAR(50) PRIMARY KEY,
    name            VARCHAR(255),
    website         VARCHAR(255),
    industry        VARCHAR(100),
    manager         VARCHAR(100),
    is_active       BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.campaigns (
    id              VARCHAR(50) PRIMARY KEY,
    client_id       VARCHAR(50) REFERENCES dds.clients(id),
    platform        VARCHAR(50),
    type            VARCHAR(50),
    start_date      DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.fact_advertising (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    campaign_id     VARCHAR(50) REFERENCES dds.campaigns(id),
    impressions     INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    cost            DECIMAL(18, 2) DEFAULT 0.0,
    conversions     INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (report_date, campaign_id) 
);

CREATE TABLE IF NOT EXISTS dds.fact_site_health (
    id              SERIAL PRIMARY KEY,
    check_date      DATE NOT NULL,
    client_id       VARCHAR(50) REFERENCES dds.clients(id),
    load_time_ms    INTEGER,
    uptime_pct      DECIMAL(5, 2),
    server_errors   INTEGER,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (check_date, client_id)
);

CREATE TABLE IF NOT EXISTS dm.client_kpi (
    report_month        DATE,
    client_name         VARCHAR(255),
    industry            VARCHAR(100),
    total_spend         DECIMAL(18, 2),
    total_conversions   INTEGER,
    cpl                 DECIMAL(18, 2),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_month, client_name)
);

CREATE TABLE IF NOT EXISTS dm.platform_stats (
    report_date         DATE,
    platform            VARCHAR(50),
    avg_ctr             DECIMAL(10, 2),
    avg_cpc             DECIMAL(10, 2),
    clicks              INTEGER,
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, platform)
);

CREATE TABLE IF NOT EXISTS dm.site_reliability (
    report_date         DATE,
    website             VARCHAR(255),
    avg_load_time       INTEGER,
    total_errors        INTEGER,
    status              VARCHAR(20),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, website)
);

CREATE TABLE IF NOT EXISTS dm.agency_finance (
    report_month        DATE,
    manager             VARCHAR(100),
    active_clients      INTEGER,
    turnover            DECIMAL(18, 2),
    commission          DECIMAL(18, 2), 
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_month, manager)
);
