{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id'],
    merge_update_columns=['name_short_ru', 'name_short_en', 'inn', 'ogrn', 'state_reg_num', 'kpp',
    'okpo', 'bik', 'opf_code', 'okato_code', 'lei_code', 'auth_capital', 'address']
  )
}}
SELECT DISTINCT ON (basis_company_id)
    basis_company_id AS id,
    name_short_ru,
    name_short_en,
    inn,
    ogrn,
    state_reg_num,
    kpp,
    okpo,
    bik,
    opf_code,
    okato_code,
    lei_code,
    auth_capital,
    address
FROM {{ ref('moex_companies') }}
ORDER BY id, update_time