{% snapshot rudata_emitents %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='id',
      check_cols=['id_emitent', 'inn']
    )
}}

select DISTINCT ON (fininstid::bigint)
    id_emitent::bigint,
    fininstid::bigint id,
    shortname_rus,
    shortname_eng,
    inn,
    okpo,
    ogrn,
    code,
    sector,
    legal_address,
    phone,
    www,
    update_date,
    fullname_rus_nrd,
    fullname_eng_nrd,
    shortname_rus_nrd,
    shortname_eng_nrd,
    company_type_short_name,
    state_reg_num,
    state_reg_date,
    state_reg_name,
    egrul_date,
    egrul_organ_name,
    country,
    lei_code,
    bik,
    fax,
    e_mail,
    credit_cmp,
    is_bank_4_non_resident,
    swift, country_oksm,
    country_oksm_cbr,
    country_name_rus,
    country_name_eng,
    region_inn,
    region_soato,
    region_name,
    fullname_rus,
    sparkid,
    br_fcsm_reg_code, capital, capital_unit, okved, oecd_lvl, oktmo, okato, post_address, okopf, okogu,
    have_rating, have_risk, is_finorg, is_control_by_finorg, is_gov_or_cb, on_behalf_of_state, is_subjectrf, is_cis_reg,
    issuer_nrd, tin, kpp, state_reg_number, market_id_nrd, market_name_rus_nrd, market_name_eng_nrd, sp_rx_entity_id,
    sp_shortname, mds_shortname, mds_org_id, fch_id, fch_shortname, is_monopoly, is_strategic, ifo_list, isregion, sic,
    sector4212u, sna2008, reg_code, reg_date, reg_org, note, primary_reg_date, inn_fle, other_tin, other_tin_name,
    okfs_id, okfs_name, is_branch, counter, rn
from {{ ref('stg_rudata_emitents') }}
order by id, id_emitent::bigint, inn

{% endsnapshot %}
