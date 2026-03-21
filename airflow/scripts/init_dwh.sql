CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS stg.clients (
    id TEXT,
    name TEXT,
    website TEXT,
    site_id TEXT,
    site_name TEXT,
    site_url TEXT,
    industry TEXT,
    manager TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.campaigns (
    id TEXT,
    client_id TEXT,
    platform TEXT,
    type TEXT,
    start_date TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.ad_stats (
    date TEXT,
    campaign_id TEXT,
    impressions TEXT,
    clicks TEXT,
    cost TEXT,
    conversions TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.site_monitoring (
    date TEXT,
    site_id TEXT,
    site_name TEXT,
    site_url TEXT,
    load_time_ms TEXT,
    uptime_pct TEXT,
    server_errors TEXT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.clients (
    client_sk SERIAL PRIMARY KEY,
    id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    industry VARCHAR(100),
    manager VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.sites (
    site_sk SERIAL PRIMARY KEY,
    site_id VARCHAR(50) UNIQUE NOT NULL,
    client_sk INTEGER REFERENCES dds.clients(client_sk) NOT NULL,
    site_name VARCHAR(255),
    site_url VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (client_sk)
);

CREATE TABLE IF NOT EXISTS dds.campaigns (
    campaign_sk SERIAL PRIMARY KEY,
    id VARCHAR(50) UNIQUE NOT NULL,
    client_sk INTEGER REFERENCES dds.clients(client_sk) NOT NULL,
    platform VARCHAR(50),
    type VARCHAR(50),
    start_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dds.fact_advertising (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    campaign_sk INTEGER REFERENCES dds.campaigns(campaign_sk) NOT NULL,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cost DECIMAL(18, 2) DEFAULT 0.0,
    conversions INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (report_date, campaign_sk)
);

CREATE TABLE IF NOT EXISTS dds.fact_site_health (
    id SERIAL PRIMARY KEY,
    check_date DATE NOT NULL,
    site_sk INTEGER REFERENCES dds.sites(site_sk) NOT NULL,
    load_time_ms INTEGER,
    uptime_pct DECIMAL(5, 2),
    server_errors INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (check_date, site_sk)
);

CREATE TABLE IF NOT EXISTS dm.client_kpi (
    report_month DATE,
    client_sk INTEGER REFERENCES dds.clients(client_sk),
    client_id VARCHAR(50),
    client_name VARCHAR(255),
    industry VARCHAR(100),
    total_spend DECIMAL(18, 2),
    total_conversions INTEGER,
    cpl DECIMAL(18, 2),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_month, client_sk)
);

CREATE TABLE IF NOT EXISTS dm.platform_stats (
    report_date DATE,
    platform VARCHAR(50),
    avg_ctr DECIMAL(10, 2),
    avg_cpc DECIMAL(10, 2),
    clicks INTEGER,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, platform)
);

CREATE TABLE IF NOT EXISTS dm.site_reliability (
    report_date DATE,
    site_sk INTEGER REFERENCES dds.sites(site_sk),
    site_id VARCHAR(50),
    site_name VARCHAR(255),
    site_url VARCHAR(255),
    avg_load_time INTEGER,
    total_errors INTEGER,
    status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, site_sk)
);

CREATE TABLE IF NOT EXISTS dm.agency_finance (
    report_month DATE,
    manager VARCHAR(100),
    active_clients INTEGER,
    turnover DECIMAL(18, 2),
    commission DECIMAL(18, 2), 
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_month, manager)
);
