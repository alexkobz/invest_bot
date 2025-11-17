{% snapshot rudata_stocks %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='id',
      check_cols=['secid']
    )
}}

select DISTINCT ON ("fintoolId"::bigint)
    "fintoolId"::bigint AS id,
    UPPER(secid) secid,
    shortname,
    regnumber,
    name,
    UPPER(isin) isin,
    is_traded::boolean,
    emitent_id,
    emitent_title,
    emitent_inn,
    emitent_okpo,
    gosreg,
    type,
    grp,
    primary_boardid,
    marketprice_boardid,
    counter,
    rn
from {{ ref('stg_rudata_stocks') }}
order by id, secid, is_traded::boolean desc, rn desc

{% endsnapshot %}
