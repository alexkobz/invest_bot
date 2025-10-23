{% snapshot moex_companies %}
{{
    config(
      target_schema='snapshots',
      strategy='timestamp',
      unique_key='basis_company_id',
      updated_at='update_time'
    )
}}

SELECT
    cast(basis_company_id as bigint) basis_company_id,
    name_short_ru,
    name_full_ru,
    name_short_en,
    name_full_en,
    name_short_tr,
    name_full_tr,
    inn,
    tin_authority_name_short_ru,
    ogrn,
    egrul_date,
    egrul_authority_name_short_ru,
    state_reg_num,
    state_reg_date,
    state_reg_authority_name_short_ru,
    cmp_code_fcsm, kpp, kpp_date,
    kpp_authority_name_short_ru,
    okpo,
    bik,
    opf_code,
    okato_code,
    lei_code,
    lei_assignment_date,
    lei_authority_name_short_ru,
    auth_capital,
    address,
    post_address,
    phone,
    fax,
    e_mail,
    www,
    cast(is_resident as boolean) is_resident,
    cast(credit_cmp as boolean) credit_cmp,
    cast(is_bank_4_non_resident as boolean) is_bank_4_non_resident,
    cast(update_time as timestamp) update_time
FROM {{ ref('stg_moex_companies') }}

{% endsnapshot %}
