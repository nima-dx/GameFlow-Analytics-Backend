-- LEAGUES
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.leagues`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/leagues/*.parquet']
);

-- SEASONS
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.seasons`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/seasons/*.parquet']
);

-- TEAMS
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.teams`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/teams/*.parquet']
);

-- PLAYERS
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.players`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/players/*.parquet']
);

-- EVENTS
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.events`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/events/*.parquet']
);

-- EVENT STATS
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.event_stats`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/event_stats/*.parquet']
);

-- EVENT TIMELINE
CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.event_timeline`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/event_timeline/*.parquet']
);
