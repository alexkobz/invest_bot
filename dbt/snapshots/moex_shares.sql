{% snapshot moex_shares %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='id',
      check_cols=['secid', 'boardid', 'issuesize']
    )
}}

select
    md5(secid || boardid || coalesce(issuesize::text, '')) id,
    upper(secid) as secid,
    upper(boardid) as boardid,
    cast(issuesize as bigint) as issuesize,
    shortname,
    prevprice,
    cast(lotsize as double precision) as lotsize,
    cast(facevalue as double precision) as facevalue,
    status,
    boardname,
    decimals,
    secname,
    remarks,
    marketcode,
    instrid,
    sectorid,
    minstep,
    prevwaprice,
    faceunit,
    prevdate,
    isin,
    latname,
    regnumber,
    prevlegalcloseprice,
    currencyid,
    sectype,
    listlevel,
    cast(settledate as date) as settledate
from {{ ref('stg_moex_shares') }}
where issuesize is not null

{% endsnapshot %}
