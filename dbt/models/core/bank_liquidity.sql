{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['DT'],
    merge_update_columns=['StrLiDefNew', 'StrLiDef', 'claims', 'actionBasedRepoFX', 'actionBasedSecureLoans',
    'standingFacilitiesRepoFX', 'standingFacilitiesSecureLoans', 'liabilities', 'depositAuctionBased',
    'depositStandingFacilities', 'CBRbonds', 'netCBRclaims', 'CorrAcc', 'AVGRR']
  )
}}
SELECT
    "DT"::timestamp,
    "StrLiDefNew"::double precision,
    "StrLiDef"::double precision,
    claims::double precision,
    "actionBasedRepoFX"::double precision,
    "actionBasedSecureLoans"::double precision,
    "standingFacilitiesRepoFX"::double precision,
    "standingFacilitiesSecureLoans"::double precision,
    liabilities::double precision,
    "depositAuctionBased"::double precision,
    "depositStandingFacilities"::double precision,
    "CBRbonds", "netCBRclaims"::double precision,
    "CorrAcc"::double precision,
    "AVGRR"::double precision
FROM {{ ref('stg_cbr_bank_liquidity') }}

