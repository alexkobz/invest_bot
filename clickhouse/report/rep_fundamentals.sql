CREATE MATERIALIZED VIEW mv_fundamentals
TO rep_fundamentals AS
SELECT
    f.*,
    -- Liquidity
    (f."1200" - f."1500")/nullif(f."1200", 0) AS working_capital_assets,
    (f."1200")/nullif(f."1500", 0) AS current_ratio,
    (f."1200" - f."1210")/nullif(f."1500", 0) AS quick_ratio,
    (f."1230" + f."1240" + f."1250")/nullif(f."1500", 0) AS quick_assets_ratio,
    (f."1250")/nullif(f."1500", 0) AS cash_ratio,
    (f."1240" + f."1250")/nullif(f."1500", 0) AS absolute_leverage_ratio,
    -- Profitability
    (f."2100")/nullif(f."2110", 0)*100 AS gross_profit_margin,
    (f."2400")/nullif(f."2110", 0)*100 AS net_profit_margin,
    (f."2200")/nullif(f."2110", 0)*100 AS operating_profit_margin,
    (f."2400")/nullif(f."1600", 0) AS return_on_assets,
    (f."2400")/nullif(f."1300", 0) AS return_on_equity,
    (f."2300")/nullif(f."1300" + f."1400", 0) AS return_on_capital_employed,
    (f."2400")/nullif(f."1300" + f."1400", 0) AS return_on_investments,
    (f."2300")/nullif(f."1300" + f."1410" + f."1510", 0) AS return_on_invested_capital,
    (f."2300")/nullif(f."2110", 0) AS return_on_sales,
    (f."2400")/nullif(argMax(sec.issuesize, f."year") OVER (PARTITION BY f.inn ORDER BY f."year"), 0) AS earnings_per_share,
    -- Efficiency
    (f."1230")*365/nullif(f."2110", 0) AS debtors_turnover,
    (f."1230")*365/nullif(f."2120", 0) AS total_operating_costs,
    (f."2120")/nullif(f."1210", 0) AS inventory_turnover,
    (f."2100")/nullif(f."1600", 0) AS asset_turnover,
    (f."2110")/nullif(f."1150", 0) AS fixed_asset_turnover,
    (f."2110")/nullif(f."1230", 0) AS receivables_turnover,
    (f."2120")/nullif(f."1520", 0) AS payables_turnover,
    (f."2110")/nullif(f."1200" - f."1500", 0) AS working_capital_turnover,
    -- Solvency
    (f."1300")/nullif(f."1700", 0)*100 AS equity_ratio,
    (f."1300")/nullif(f."1600", 0)*100 AS equity_assets_ratio,
    (f."1400" + f."1500")/nullif(f."1700", 0) AS debt_ratio,
    (f."1400" + f."1500")/nullif(f."1300", 0) AS debt_capital_ratio,
    (f."1400" + f."1500")/nullif(f."1300", 0) AS financial_leverage_ratio,
    (f."1400")/nullif(f."1300", 0) AS debt_equity,
    (f."1400")/nullif(f."1600", 0) AS debt_assets,
    -- Market
    (f."2200" + f."2340" - f."2350") AS ebit,
    (f."2200" + f."2340" - f."2350")/nullif(f."2110", 0)*100 AS ebit_margin,
    (f."2200" + f."2330" + f."2340" - f."2350") AS ebitda,
    (f."2200" + f."2330" + f."2340" - f."2350")/nullif(f."2110", 0)*100 AS ebitda_margin,

    argMax(sec.secid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secid,
    argMax(sec.boardid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as boardid,
    argMax(sec.sectype, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as sectype,
    argMax(sec.secgroup, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secgroup,
    argMax(sec.issuesize, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as issuesize,
    e.sector,
    e.country
FROM v_fundamentals AS f
LEFT JOIN v_emitents AS e ON e.inn = f.inn
ASOF LEFT JOIN v_moex_securities AS sec
    ON sec.inn = f.inn
    AND year(sec.settledate) >= f."year"
SETTINGS join_use_nulls = 1
