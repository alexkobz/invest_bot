{% snapshot moex_boards %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key=['id'],
      check_cols=['board_group_id', 'boardid']
    )
}}

select
    cast(id as bigint) id,
    cast(board_group_id as bigint) board_group_id,
    upper(boardid) as boardid,
    title,
    cast(is_traded as boolean) is_traded
from {{ ref('stg_moex_boards') }}

{% endsnapshot %}
