CREATE OR REPLACE EXTERNAL TABLE `le-wagon-data-atelier.raw_dataset.leagues`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gameflow-ingestion-processed/*.parquet']
);
