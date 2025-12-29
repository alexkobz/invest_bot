{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['figi', 'date', 'time'],
    merge_update_columns=['"timestamp"', 'volume', 'is_complete', 'candle_source', 'open', 'close', 'low', 'high']
  )
}}
SELECT DISTINCT ON (UPPER(figi), "time"::date, "time"::time)
	UPPER(figi) figi, 
	"time"::date "date",
	"time"::time "time",
	"time"::timestamp "timestamp", 
	volume::bigint volume, 
	is_complete::boolean is_complete, 
	candle_source::bigint candle_source, 
	(open_units::numeric + round(open_nano::numeric / 1000000000, 2))::double precision "open",
	(close_units::numeric + round(close_nano::numeric / 1000000000, 2))::double precision "close",
	(low_units::numeric + round(low_nano::numeric / 1000000000, 2))::double precision "low",
	(high_units::numeric + round(high_nano::numeric / 1000000000, 2))::double precision "high"
FROM {{ ref('stg_tbank_historic_candles1min') }}
ORDER BY UPPER(figi), "time"::date DESC, "time"::time DESC
