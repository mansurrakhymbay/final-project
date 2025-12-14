CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.breeds (
  id          TEXT PRIMARY KEY,
  name        TEXT,
  origin      TEXT,
  temperament TEXT,
  life_span   TEXT,
  raw_json    JSONB NOT NULL,
  loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.images (
  id          TEXT PRIMARY KEY,
  breed_id    TEXT,
  url         TEXT,
  width       INT,
  height      INT,
  created_at  TIMESTAMPTZ,
  raw_json    JSONB NOT NULL,
  loaded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_images_breed_id ON raw.images(breed_id);
