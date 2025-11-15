{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['dt', 'str_li_def_new', 'str_li_def', 'claims', 'action_based_repo_fx', 'action_based_secure_loans',
    'standing_facilities_repo_fx', 'standing_facilities_secure_loans', 'liabilities', 'deposit_auction_based',
    'deposit_standing_facilities', 'cbr_bonds', 'net_cbr_claims', 'corr_acc', 'avgrr']
  )
}}
SELECT DISTINCT ON ("DT"::date)
    "DT"::date date,
    "DT"::timestamp dt,
    "StrLiDefNew"::double precision str_li_def_new,
    "StrLiDef"::double precision str_li_def,
    claims::double precision claims,
    "actionBasedRepoFX"::double precision action_based_repo_fx,
    "actionBasedSecureLoans"::double precision action_based_secure_loans,
    "standingFacilitiesRepoFX"::double precision standing_facilities_repo_fx,
    "standingFacilitiesSecureLoans"::double precision standing_facilities_secure_loans,
    liabilities::double precision liabilities,
    "depositAuctionBased"::double precision deposit_auction_based,
    "depositStandingFacilities"::double precision deposit_standing_facilities,
    "CBRbonds"::double precision cbr_bonds,
    "netCBRclaims"::double precision net_cbr_claims,
    "CorrAcc"::double precision corr_acc,
    "AVGRR"::double precision avgrr
FROM {{ ref('stg_cbr_bank_liquidity') }}
ORDER BY "DT"::date