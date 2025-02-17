{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["inn", "year"],
    merge_update_columns=[
'inn', '"year"', '"1100"', '"1110"', '"1120"', '"1130"', '"1140"', '"1150"', '"1160"', '"1170"', '"1180"', '"1190"',
'"1200"', '"1210"', '"1220"', '"1230"', '"1240"', '"1250"', '"1260"', '"1300"', '"1310"', '"1320"', '"1340"', '"1350"',
'"1360"', '"1370"', '"1400"', '"1410"', '"1420"', '"1430"', '"1450"', '"1500"', '"1510"', '"1520"', '"1530"', '"1540"',
'"1550"', '"1600"', '"1700"', '"2100"', '"2110"', '"2120"', '"2200"', '"2210"', '"2220"', '"2300"', '"2310"', '"2320"',
'"2330"', '"2340"', '"2350"', '"2400"', '"2410"', '"2421"', '"2430"', '"2450"', '"2460"', '"2500"', '"2510"', '"2520"',
'"3200"', '"3300"', '"3310"', '"3311"', '"3312"', '"3313"', '"3314"', '"3315"', '"3316"', '"3320"', '"3321"', '"3322"',
'"3323"', '"3324"', '"3325"', '"3326"', '"3327"', '"3330"', '"3340"', '"3600"', '"4100"', '"4110"', '"4111"', '"4112"',
'"4113"', '"4119"', '"4120"', '"4121"', '"4122"', '"4123"', '"4124"', '"4129"', '"4200"', '"4210"', '"4211"', '"4212"',
'"4213"', '"4214"', '"4219"', '"4220"', '"4221"', '"4222"', '"4223"', '"4224"', '"4229"', '"4300"', '"4310"', '"4311"',
'"4312"', '"4313"', '"4314"', '"4319"', '"4320"', '"4321"', '"4322"', '"4323"', '"4329"', '"4400"', '"4490"', '"6100"',
'"6200"', '"6210"', '"6215"', '"6220"', '"6230"', '"6240"', '"6250"', '"6300"', '"6310"', '"6311"', '"6312"', '"6313"',
'"6320"', '"6321"', '"6322"', '"6323"', '"6324"', '"6325"', '"6326"', '"6330"', '"6350"', '"6400"', '"2411"', '"4450"',
'"4500"', '"2412"', '"3100"', '"3210"', '"3211"', '"3230"', '"3400"', '"3401"', '"3402"', '"3500"', '"3501"', '"3502"',
'"3220"', '"3221"', '"3410"', '"3411"', '"2900"', '"2910"', '"3213"', '"3227"', '"3223"', '"3420"', '"3421"', '"3240"',
'"3212"', '"3222"', '"2530"', '"3216"', '"3226"', '"3214"', '"3215"', '"3224"', '"3225"', '"3412"', '"3422"', '"date"',
'"type"']
  )
}}
WITH stg AS (
    SELECT
        *,
        row_number() OVER(PARTITION BY inn, cast("year" AS INTEGER) ORDER BY cast(reporting_year AS INTEGER) DESC) AS rn
    FROM {{ ref("stg_girbo_fundamentals") }}
)
SELECT
    inn,
    cast("year" AS INTEGER) AS "year",
    "1100",
    "1110",
    "1120",
    "1130",
    "1140",
    "1150",
    "1160",
    "1170",
    "1180",
    "1190",
    "1200",
    "1210",
    "1220",
    "1230",
    "1240",
    "1250",
    "1260",
    "1300",
    "1310",
    "1320",
    "1340",
    "1350",
    "1360",
    "1370",
    "1400",
    "1410",
    "1420",
    "1430",
    "1450",
    "1500",
    "1510",
    "1520",
    "1530",
    "1540",
    "1550",
    "1600",
    "1700",
    "2100",
    "2110",
    "2120",
    "2200",
    "2210",
    "2220",
    "2300",
    "2310",
    "2320",
    "2330",
    "2340",
    "2350",
    "2400",
    "2410",
    "2421",
    "2430",
    "2450",
    "2460",
    "2500",
    "2510",
    "2520",
    "3200",
    "3300",
    "3310",
    "3311",
    "3312",
    "3313",
    "3314",
    "3315",
    "3316",
    "3320",
    "3321",
    "3322",
    "3323",
    "3324",
    "3325",
    "3326",
    "3327",
    "3330",
    "3340",
    "3600",
    "4100",
    "4110",
    "4111",
    "4112",
    "4113",
    "4119",
    "4120",
    "4121",
    "4122",
    "4123",
    "4124",
    "4129",
    "4200",
    "4210",
    "4211",
    "4212",
    "4213",
    "4214",
    "4219",
    "4220",
    "4221",
    "4222",
    "4223",
    "4224",
    "4229",
    "4300",
    "4310",
    "4311",
    "4312",
    "4313",
    "4314",
    "4319",
    "4320",
    "4321",
    "4322",
    "4323",
    "4329",
    "4400",
    "4490",
    "6100",
    "6200",
    "6210",
    "6215",
    "6220",
    "6230",
    "6240",
    "6250",
    "6300",
    "6310",
    "6311",
    "6312",
    "6313",
    "6320",
    "6321",
    "6322",
    "6323",
    "6324",
    "6325",
    "6326",
    "6330",
    "6350",
    "6400",
    "2411",
    "4450",
    "4500",
    "2412",
    "3100",
    "3210",
    "3211",
    "3230",
    "3400",
    "3401",
    "3402",
    "3500",
    "3501",
    "3502",
    "3220",
    "3221",
    "3410",
    "3411",
    "2900",
    "2910",
    "3213",
    "3227",
    "3223",
    "3420",
    "3421",
    "3240",
    "3212",
    "3222",
    "2530",
    "3216",
    "3226",
    "3214",
    "3215",
    "3224",
    "3225",
    "3412",
    "3422",
    CAST(NOW() AS VARCHAR) AS "date",
    0 AS "type"
FROM stg
WHERE rn = 1