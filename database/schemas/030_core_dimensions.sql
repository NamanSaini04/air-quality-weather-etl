-- 030_core_dimensions.sql

-- Cities we track. Seeded from config/cities.yaml by `python main.py seed-locations`.
CREATE TABLE IF NOT EXISTS core.dim_location (
    location_id   SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city_key      TEXT        NOT NULL UNIQUE,       -- stable natural key: 'london'
    city_name     TEXT        NOT NULL,
    region        TEXT,
    country_code  CHAR(2)     NOT NULL,
    latitude      NUMERIC(8,5) NOT NULL CHECK (latitude  BETWEEN -90  AND 90),
    longitude     NUMERIC(8,5) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    timezone      TEXT        NOT NULL,              -- IANA name, e.g. 'Asia/Kolkata'
    climate_zone  TEXT        NOT NULL,              -- drives season mapping (UK vs India differ)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Monitoring stations. Air quality is measured at stations, not "cities".
-- A city can have many stations. If we use a modelled source (no stations),
-- we create one 'model_grid' station per city so the schema stays the same.
CREATE TABLE IF NOT EXISTS core.dim_station (
    station_id         INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id        SMALLINT    NOT NULL REFERENCES core.dim_location (location_id),
    source             TEXT        NOT NULL,
    source_station_id  TEXT        NOT NULL,         -- the ID the API uses
    station_name       TEXT,
    latitude           NUMERIC(8,5) CHECK (latitude  BETWEEN -90  AND 90),
    longitude          NUMERIC(8,5) CHECK (longitude BETWEEN -180 AND 180),
    station_type       TEXT        NOT NULL
                       CHECK (station_type IN ('reference_monitor', 'low_cost_sensor', 'model_grid')),
    is_active          BOOLEAN     NOT NULL DEFAULT TRUE,
    first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_station_id)
);
CREATE INDEX IF NOT EXISTS ix_station_location ON core.dim_station (location_id);

-- Pollutant reference. Canonical unit is µg/m³ for everything.
-- molecular_weight is needed to convert ppb/ppm -> µg/m³ for gases (Phase 4).
CREATE TABLE IF NOT EXISTS core.dim_pollutant (
    pollutant_code          TEXT PRIMARY KEY,        -- 'pm25', 'no2', ...
    display_name            TEXT    NOT NULL,
    canonical_unit          TEXT    NOT NULL DEFAULT 'µg/m³',
    molecular_weight_g_mol  NUMERIC(6,2),            -- NULL for particulates
    is_particulate          BOOLEAN NOT NULL
);

INSERT INTO core.dim_pollutant
    (pollutant_code, display_name, molecular_weight_g_mol, is_particulate)
VALUES
    ('pm25', 'PM2.5',             NULL,  TRUE),
    ('pm10', 'PM10',              NULL,  TRUE),
    ('no2',  'Nitrogen dioxide',  46.01, FALSE),
    ('so2',  'Sulphur dioxide',   64.07, FALSE),
    ('o3',   'Ozone',             48.00, FALSE),
    ('co',   'Carbon monoxide',   28.01, FALSE)
ON CONFLICT (pollutant_code) DO NOTHING;

-- Calendar dimension at DAY grain. Hour lives on the fact tables.
-- Season is NOT here on purpose: seasons depend on location
-- (UK winter = Dec-Feb; North India post-monsoon = Oct-Nov). That is derived in dbt.
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_key      INTEGER PRIMARY KEY,               -- YYYYMMDD, e.g. 20261103
    full_date     DATE     NOT NULL UNIQUE,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,                 -- ISO: 1 = Monday ... 7 = Sunday
    day_name      TEXT     NOT NULL,
    is_weekend    BOOLEAN  NOT NULL,
    iso_week      SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    TEXT     NOT NULL,
    quarter       SMALLINT NOT NULL,
    year          SMALLINT NOT NULL
);

INSERT INTO core.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(DAY     FROM d)::SMALLINT,
    EXTRACT(ISODOW  FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(ISODOW  FROM d) IN (6, 7),
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR    FROM d)::SMALLINT
FROM generate_series('2022-01-01'::DATE, '2030-12-31'::DATE, INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
