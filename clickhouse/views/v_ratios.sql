CREATE VIEW v_ratios AS
with ratios_year AS (
	SELECT *
	FROM ratios
	WHERE period = 'Y'
)
, generated_years AS (
    SELECT
        code,
        type,
        arrayJoin(range(min_year, toYear(current_date()) + 1)) AS year
    FROM (
        SELECT code, type, MIN(year) AS min_year FROM ratios_year GROUP BY code, type
    )
)
, sec AS (
	SELECT secid, inn
	FROM (
		SELECT secid, inn,
			row_number() OVER (PARTITION BY secid ORDER BY settledate) rn
		FROM v_moex_securities
	)
	WHERE rn = 1
)
SELECT
    g.code AS code,
    g."year" AS "year",
    g.type AS type,
    argMax(r.active, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS active,
    argMax(r.capex_revenue, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS capex_revenue,
    argMax(r.capital, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS capital,
    argMax(r.changed_at, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS changed_at,
    argMax(r.current_ratio, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS current_ratio,
    argMax(r.debt_equity, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS debt_equity,
    argMax(r.debt_ratio, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS debt_ratio,
    argMax(r.debtebitda, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS debtebitda,
    argMax(r.dpr, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS dpr,
    argMax(r.ebitda_margin, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS ebitda_margin,
    argMax(r.ev_ebit, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS ev_ebit,
    argMax(r.evebitda, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS evebitda,
    argMax(r.evs, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS evs,
    argMax(r.exchange, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS exchange,
    argMax(r.gross_margin, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS gross_margin,
    argMax(r.interest_coverage, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS interest_coverage,
    argMax(r."month", g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS "month",
    argMax(r.net_margin, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS net_margin,
    argMax(r.net_working_capital, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS net_working_capital,
    argMax(r.netdebt_ebitda, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS netdebt_ebitda,
    argMax(r.operation_margin, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS operation_margin,
    argMax(r.pbv, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS pbv,
    argMax(r.pcf, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS pcf,
    argMax(r.pe, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS pe,
    argMax(r.period, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS period,
    argMax(r.pfcf, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS pfcf,
    argMax(r.pffo, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS pffo,
    argMax(r.ps, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS ps,
    argMax(r.roa, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS roa,
    argMax(r.roce, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS roce,
    argMax(r.roe, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS roe,
    argMax(r.roic, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS roic,
    argMax(r.ros, g."year") OVER (PARTITION BY g.code, g.type ORDER BY g."year") AS ros,
    sec.inn AS inn,
	f."1100",
	f."1110",
	f."1120",
	f."1130",
	f."1140",
	f."1150",
	f."1160",
	f."1170",
	f."1180",
	f."1190",
	f."1200",
	f."1210",
	f."1220",
	f."1230",
	f."1240",
	f."1250",
	f."1260",
	f."1300",
	f."1310",
	f."1320",
	f."1340",
	f."1350",
	f."1360",
	f."1370",
	f."1400",
	f."1410",
	f."1420",
	f."1430",
	f."1450",
	f."1500",
	f."1510",
	f."1520",
	f."1530",
	f."1540",
	f."1550",
	f."1600",
	f."1700",
	f."2100",
	f."2110",
	f."2120",
	f."2200",
	f."2210",
	f."2220",
	f."2300",
	f."2310",
	f."2320",
	f."2330",
	f."2340",
	f."2350",
	f."2400",
	f."2410",
	f."2421",
	f."2430",
	f."2450",
	f."2460",
	f."2500",
	f."2510",
	f."2520",
	f."3200",
	f."3300",
	f."3310",
	f."3311",
	f."3312",
	f."3313",
	f."3314",
	f."3315",
	f."3316",
	f."3320",
	f."3321",
	f."3322",
	f."3323",
	f."3324",
	f."3325",
	f."3326",
	f."3327",
	f."3330",
	f."3340",
	f."3600",
	f."4100",
	f."4110",
	f."4111",
	f."4112",
	f."4113",
	f."4119",
	f."4120",
	f."4121",
	f."4122",
	f."4123",
	f."4124",
	f."4129",
	f."4200",
	f."4210",
	f."4211",
	f."4212",
	f."4213",
	f."4214",
	f."4219",
	f."4220",
	f."4221",
	f."4222",
	f."4223",
	f."4224",
	f."4229",
	f."4300",
	f."4310",
	f."4311",
	f."4312",
	f."4313",
	f."4314",
	f."4319",
	f."4320",
	f."4321",
	f."4322",
	f."4323",
	f."4329",
	f."4400",
	f."4490",
	f."6100",
	f."6200",
	f."6210",
	f."6215",
	f."6220",
	f."6230",
	f."6240",
	f."6250",
	f."6300",
	f."6310",
	f."6311",
	f."6312",
	f."6313",
	f."6320",
	f."6321",
	f."6322",
	f."6323",
	f."6324",
	f."6325",
	f."6326",
	f."6330",
	f."6350",
	f."6400",
	f."2411",
	f."4450",
	f."4500",
	f."2412",
	f."3100",
	f."3210",
	f."3211",
	f."3230",
	f."3400",
	f."3401",
	f."3402",
	f."3500",
	f."3501",
	f."3502",
	f."3220",
	f."3221",
	f."3410",
	f."3411",
	f."2900",
	f."2910",
	f."3213",
	f."3227",
	f."3223",
	f."3420",
	f."3421",
	f."3240",
	f."3212",
	f."3222",
	f."2530",
	f."3216",
	f."3226",
	f."3214",
	f."3215",
	f."3224",
	f."3225",
	f."3412",
	f."3422"
FROM generated_years AS g
LEFT JOIN ratios_year AS r
    ON g.code = r.code AND g.type = r.type AND g.year = r.year
LEFT JOIN sec ON sec.secid = g.code
LEFT JOIN v_fundamentals f ON f.inn = sec.inn AND g."year" = f."year"
SETTINGS join_use_nulls = 1
