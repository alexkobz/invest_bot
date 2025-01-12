{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='inn',
    merge_update_columns=['card']
  )
}}
SELECT
    inn,
    card
FROM {{ ref('stg_girbo_organizations_cards') }}