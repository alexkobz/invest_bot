{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id_emitent', 'inn'],
    merge_update_columns=['fininstid', 'shortname_rus', 'shortname_eng', 'okpo', 'ogrn',
    'code', 'sector', 'legal_address', 'phone', 'www', 'update_date', 'fullname_rus_nrd', 'fullname_eng_nrd',
    'shortname_rus_nrd', 'shortname_eng_nrd', 'company_type_short_name', 'state_reg_num', 'state_reg_date',
    'state_reg_name', 'egrul_date', 'egrul_organ_name', 'country', 'lei_code', 'bik', 'fax', 'e_mail',
    'credit_cmp', 'is_bank_4_non_resident', 'swift', 'country_oksm', 'country_oksm_cbr', 'country_name_rus',
    'country_name_eng', 'region_inn', 'region_soato', 'region_name', 'fullname_rus', 'sparkid',
    'br_fcsm_reg_code', 'capital', 'capital_unit', 'okved', 'oecd_lvl', 'oktmo', 'okato', 'post_address',
    'okopf', 'okogu', 'have_rating', 'have_risk', 'is_finorg', 'is_control_by_finorg', 'is_gov_or_cb',
    'on_behalf_of_state', 'is_subjectrf', 'is_cis_reg', 'issuer_nrd', 'tin', 'kpp', 'state_reg_number',
    'market_id_nrd', 'market_name_rus_nrd', 'market_name_eng_nrd', 'sp_rx_entity_id', 'sp_shortname',
    'mds_shortname', 'mds_org_id', 'fch_id', 'fch_shortname', 'is_monopoly', 'is_strategic', 'ifo_list',
    'isregion', 'sic', 'sector4212u', 'sna2008', 'reg_code', 'reg_date', 'reg_org', 'note', 'primary_reg_date',
    'inn_fle', 'other_tin', 'other_tin_name', 'okfs_id', 'okfs_name', 'is_branch']
  )
}}
WITH moex AS (
    SELECT * FROM (
	    SELECT
	        emitent_id,
	        emitent_inn,
	        emitent_title,
	        emitent_okpo,
	        gosreg,
	        row_number() OVER(PARTITION BY emitent_inn ORDER BY emitent_id DESC) AS rn
	    FROM {{ ref('stg_moex_securities_trading') }}
	)
	WHERE rn = 1
)
SELECT
    coalesce(CAST(rudata.id_emitent AS BIGINT), CAST(moex.emitent_id AS BIGINT), 0) AS id_emitent,
    CAST(rudata.fininstid AS BIGINT) AS fininstid,
    rudata.shortname_rus,
    rudata.shortname_eng,
    coalesce(rudata.inn, moex.emitent_inn, '') AS inn,
    rudata.okpo,
    rudata.ogrn,
    rudata.code,
    rudata.sector,
    rudata.legal_address,
    rudata.phone,
    rudata.www,
    DATE_TRUNC('second', CAST(rudata.update_date AS TIMESTAMP)) AS update_date,
    rudata.fullname_rus_nrd,
    rudata.fullname_eng_nrd,
    rudata.shortname_rus_nrd,
    rudata.shortname_eng_nrd,
    rudata.company_type_short_name,
    rudata.state_reg_num,
    DATE_TRUNC('second', CAST(rudata.state_reg_date AS TIMESTAMP)) AS state_reg_date,
    rudata.state_reg_name,
    DATE_TRUNC('second', CAST(rudata.egrul_date AS TIMESTAMP)) AS egrul_date,
    rudata.egrul_organ_name,
    rudata.country,
    rudata.lei_code,
    rudata.bik,
    rudata.fax,
    rudata.e_mail,
    rudata.credit_cmp,
    rudata.is_bank_4_non_resident,
    rudata.swift,
    CAST(rudata.country_oksm AS BIGINT) AS country_oksm,
    CAST(rudata.country_oksm_cbr AS BIGINT) AS country_oksm_cbr,
    rudata.country_name_rus,
    rudata.country_name_eng,
    rudata.region_inn,
    rudata.region_soato,
    rudata.region_name,
    coalesce(rudata.fullname_rus, moex.emitent_title, '') AS fullname_rus,
    CAST(rudata.sparkid AS BIGINT) AS sparkid,
    rudata.br_fcsm_reg_code,
    CAST(rudata.capital AS BIGINT) AS capital,
    rudata.capital_unit,
    rudata.okved,
    rudata.oecd_lvl,
    rudata.oktmo,
    rudata.okato,
    rudata.post_address,
    rudata.okopf,
    rudata.okogu,
    (rudata.have_rating != 0)::BOOLEAN AS have_rating,
    (rudata.have_risk != 0)::BOOLEAN AS have_risk,
    (rudata.is_finorg != 0)::BOOLEAN AS is_finorg,
    (rudata.is_control_by_finorg != 0)::BOOLEAN AS is_control_by_finorg,
    (rudata.is_gov_or_cb != 0)::BOOLEAN AS is_gov_or_cb,
    (rudata.on_behalf_of_state != 0)::BOOLEAN AS on_behalf_of_state,
    (rudata.is_subjectrf != 0)::BOOLEAN AS is_subjectrf,
    (rudata.is_cis_reg != 0)::BOOLEAN AS is_cis_reg,
    rudata.issuer_nrd,
    rudata.tin,
    rudata.kpp,
    rudata.state_reg_number,
    CAST(rudata.market_id_nrd AS BIGINT) AS market_id_nrd,
    rudata.market_name_rus_nrd,
    rudata.market_name_eng_nrd,
    rudata.sp_rx_entity_id,
    rudata.sp_shortname,
    rudata.mds_shortname,
    rudata.mds_org_id,
    rudata.fch_id,
    rudata.fch_shortname,
    (rudata.is_monopoly != 0)::BOOLEAN AS is_monopoly,
    (rudata.is_strategic != 0)::BOOLEAN AS is_strategic,
    rudata.ifo_list,
    (rudata.isregion != 0)::BOOLEAN AS isregion,
    rudata.sic,
    rudata.sector4212u,
    rudata.sna2008,
    coalesce(rudata.reg_code, moex.gosreg, '') AS reg_code,
    DATE_TRUNC('second', CAST(rudata.reg_date AS TIMESTAMP)) AS reg_date,
    rudata.reg_org,
    rudata.note,
    DATE_TRUNC('second', CAST(rudata.primary_reg_date AS TIMESTAMP)) AS primary_reg_date,
    rudata.inn_fle,
    rudata.other_tin,
    rudata.other_tin_name,
    rudata.okfs_id,
    rudata.okfs_name,
    rudata.is_branch::BOOLEAN AS is_branch
FROM {{ ref('stg_rudata_emitents') }} AS rudata
FULL JOIN moex ON rudata.inn = moex.emitent_inn
WHERE coalesce(rudata.inn, moex.emitent_inn) IS NOT NULL