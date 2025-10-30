{% snapshot tbank_bonds %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='figi',
      check_cols=['ticker', 'class_code', 'isin', 'lot', 'currency', 'short_enabled_flag', 'name', 'exchange',
      'coupon_quantity_per_year', 'maturity_date', 'state_reg_date', 'placement_date', 'country_of_risk',
      'country_of_risk_name', 'sector', 'issue_kind', 'issue_size', 'issue_size_plan', 'trading_status', 'otc_flag',
      'buy_available_flag', 'sell_available_flag', 'floating_coupon_flag', 'perpetual_flag', 'amortization_flag',
      'api_trade_available_flag', 'uid', 'real_exchange', 'position_uid', 'asset_uid', 'for_iis_flag',
      'for_qual_investor_flag', 'weekend_flag', 'blocked_tca_flag', 'subordinated_flag', 'liquidity_flag',
      'first_1min_candle_date', 'first_1day_candle_date', 'risk_level', 'bond_type', 'call_date', 'required_tests',
      'klong_units', 'klong_nano', 'kshort_units', 'kshort_nano', 'dlong_units', 'dlong_nano',
      'dshort_units', 'dshort_nano', 'dlong_min_units', 'dlong_min_nano', 'dshort_min_units', 'dshort_min_nano',
      'nominal_currency', 'nominal_units', 'nominal_nano', 'initial_nominal_currency', 'initial_nominal_units',
      'initial_nominal_nano', 'placement_price_currency', 'placement_price_units', 'placement_price_nano',
      'aci_value_currency', 'aci_value_units', 'aci_value_nano', 'min_price_increment_units',
      'min_price_increment_nano', 'brand_logo_name', 'brand_logo_base_color', 'brand_text_color', 'dlong_client_units',
      'dlong_client_nano', 'dshort_client_units', 'dshort_client_nano']
    )
}}

SELECT
    UPPER(figi) figi,
    UPPER(ticker) ticker,
    class_code, isin, lot, currency, short_enabled_flag, name, exchange, coupon_quantity_per_year, maturity_date,
    state_reg_date, placement_date, country_of_risk, country_of_risk_name, sector, issue_kind, issue_size,
    issue_size_plan, trading_status, otc_flag, buy_available_flag, sell_available_flag, floating_coupon_flag,
    perpetual_flag, amortization_flag, api_trade_available_flag, uid, real_exchange, position_uid, asset_uid,
    for_iis_flag, for_qual_investor_flag, weekend_flag, blocked_tca_flag, subordinated_flag, liquidity_flag,
    first_1min_candle_date, first_1day_candle_date, risk_level, bond_type, call_date, required_tests, klong_units,
    klong_nano, kshort_units, kshort_nano, dlong_units, dlong_nano, dshort_units, dshort_nano, dlong_min_units,
    dlong_min_nano, dshort_min_units, dshort_min_nano, nominal_currency, nominal_units, nominal_nano,
    initial_nominal_currency, initial_nominal_units, initial_nominal_nano, placement_price_currency,
    placement_price_units, placement_price_nano, aci_value_currency, aci_value_units, aci_value_nano,
    min_price_increment_units, min_price_increment_nano, brand_logo_name, brand_logo_base_color, brand_text_color,
    dlong_client_units, dlong_client_nano, dshort_client_units, dshort_client_nano
FROM {{ ref('stg_tbank_bonds') }}

{% endsnapshot %}
