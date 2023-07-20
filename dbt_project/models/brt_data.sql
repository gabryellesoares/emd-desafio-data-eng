{{ config(materialized='table') }}

WITH brt_data AS (
    SELECT
        id_onibus,
        latitude,
        longitude,
        velocidade
    FROM
        brt_data_pgsql
)

SELECT *
FROM brt_data;