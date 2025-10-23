{% snapshot tbank_etfs %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='figi',
      check_cols=['ticker', 'class_code', 'isin', 'lot', 'currency', 'short_enabled_flag', 'name', 'exchange',
      'focus_type', 'released_date', 'country_of_risk',
      'country_of_risk_name', 'sector', 'rebalancing_freq', 'trading_status', 'otc_flag', 'buy_available_flag', 'sell_available_flag',
      'api_trade_available_flag', 'uid', 'real_exchange', 'position_uid', 'asset_uid', 'instrument_exchange', 'required_tests',
      'for_iis_flag', 'for_qual_investor_flag', 'weekend_flag', 'blocked_tca_flag', 'liquidity_flag', 'first_1min_candle_date',
      'first_1day_candle_date', 'klong_units', 'klong_nano', 'kshort_units', 'kshort_nano', 'dlong_units', 'dlong_nano',
      'dshort_units', 'dshort_nano', 'dlong_min_units', 'dlong_min_nano', 'dshort_min_units', 'dshort_min_nano',
      'fixed_commission_units', 'fixed_commission_nano', 'num_shares_units', 'num_shares_nano', 'min_price_increment_units',
      'min_price_increment_nano', 'brand_logo_name', 'brand_logo_base_color', 'brand_text_color',
      'dlong_client_units', 'dlong_client_nano', 'dshort_client_units', 'dshort_client_nano']
    )
}}

SELECT
    UPPER(figi) figi,
    UPPER(ticker) ticker,
    class_code, isin, lot, currency, short_enabled_flag, name, exchange, focus_type, released_date, country_of_risk,
    country_of_risk_name, sector, rebalancing_freq, trading_status, otc_flag, buy_available_flag, sell_available_flag,
    api_trade_available_flag, uid, real_exchange, position_uid, asset_uid, instrument_exchange, required_tests,
    for_iis_flag, for_qual_investor_flag, weekend_flag, blocked_tca_flag, liquidity_flag, first_1min_candle_date,
    first_1day_candle_date, klong_units, klong_nano, kshort_units, kshort_nano, dlong_units, dlong_nano,
    dshort_units, dshort_nano, dlong_min_units, dlong_min_nano, dshort_min_units, dshort_min_nano,
    fixed_commission_units, fixed_commission_nano, num_shares_units, num_shares_nano, min_price_increment_units,
    min_price_increment_nano, brand_logo_name, brand_logo_base_color, brand_text_color,
    dlong_client_units, dlong_client_nano, dshort_client_units, dshort_client_nano
FROM {{ ref('stg_tbank_etfs') }}

{% endsnapshot %}
