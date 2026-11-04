-- 040_core_facts.sql
-- Grain is stated for every fact table. If you cannot say the grain in one
-- sentence, the table design is not finished.

-- GRAIN: one row per location, per hour (UTC), per weather source.
CREATE TABLE IF NOT EXISTS core.fact_weather_hourly (
    weather_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_id           SMALLINT    NOT NULL REFERENCES core.dim_location (location_id),
    date_key              INTEGER     NOT NULL REFERENCES core.dim_date (date_key),  -- LOCAL date
    observed_at_utc       TIMESTAMPTZ NOT NULL,
    local_hour            SMALLINT    NOT NULL CHECK (local_hour BETWEEN 0 AND 23),
    source                TEXT        NOT NULL,
    temperature_c         NUMERIC(5,2) CHECK (temperature_c BETWEEN -90 AND 60),
    relative_humidity_pct NUMERIC(5,2) CHECK (relative_humidity_pct BETWEEN 0 AND 100),
    pressure_hpa          NUMERIC(6,1) CHECK (pressure_hpa BETWEEN 850 AND 1100),
    precipitation_mm      NUMERIC(6,2) CHECK (precipitation_mm >= 0),
    wind_speed_kmh        NUMERIC(5,1) CHECK (wind_speed_kmh >= 0),
    wind_direction_deg    SMALLINT     CHECK (wind_direction_deg BETWEEN 0 AND 360),
    weather_code          SMALLINT,                  -- WMO code; mapped to a label in dbt
    batch_run_id          UUID        NOT NULL REFERENCES ops.pipeline_run (run_id),
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Natural key = idempotency key. UPSERTs target this constraint.
    CONSTRAINT uq_weather_obs UNIQUE (location_id, observed_at_utc, source)
);
CREATE INDEX IF NOT EXISTS ix_weather_date ON core.fact_weather_hourly (date_key);

-- GRAIN: one row per station, per pollutant, per hour (UTC).
-- Long ("tall") format on purpose: APIs return one value per sensor per pollutant.
-- A new pollutant becomes new rows, not a schema change. dbt pivots to wide later.
-- AQI is NOT stored here: AQI is calculated from concentrations, not measured.
-- Keeping it derived means a formula change is a dbt rebuild, not a reload.
CREATE TABLE IF NOT EXISTS core.fact_air_quality_measurement (
    aq_measurement_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id         INTEGER     NOT NULL REFERENCES core.dim_station (station_id),
    pollutant_code     TEXT        NOT NULL REFERENCES core.dim_pollutant (pollutant_code),
    date_key           INTEGER     NOT NULL REFERENCES core.dim_date (date_key),     -- LOCAL date
    observed_at_utc    TIMESTAMPTZ NOT NULL,          -- end of the averaging hour
    local_hour         SMALLINT    NOT NULL CHECK (local_hour BETWEEN 0 AND 23),
    value_ugm3         NUMERIC(10,3) NOT NULL CHECK (value_ugm3 >= 0),  -- canonical unit
    original_value     NUMERIC(12,4),
    original_unit      TEXT,
    source             TEXT        NOT NULL,
    batch_run_id       UUID        NOT NULL REFERENCES ops.pipeline_run (run_id),
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_aq_obs UNIQUE (station_id, pollutant_code, observed_at_utc)
);
CREATE INDEX IF NOT EXISTS ix_aq_date ON core.fact_air_quality_measurement (date_key);
