KEEP_COLS = [
    # Property identifiers: 6
    "parid",
    "boro",
    "block",
    "lot",
    "easement",
    "rectype",

    # Assessment year and snapshot: 3
    "year",
    "period",
    "extracrdt",

    # Address and local geography: 6
    "housenum_lo",
    "housenum_hi",
    "street_name",
    "zip_code",
    "cpb_boro",
    "cpb_dist",

    # Property characteristics: 11
    "bldg_class",
    "bld_story",
    "num_bldgs",
    "yrbuilt",
    "yrbuilt_flag",
    "yralt1",
    "yralt2",
    "units",
    "coop_apts",
    "land_area",
    "gross_sqft",

    # Building use: 6
    "residential_area_gross",
    "office_area_gross",
    "retail_area_gross",
    "hotel_area_gross",
    "factory_area_gross",
    "warehouse_area_gross",

    # Final assessment values: 9
    "finmktland",
    "finmkttot",
    "finacttot",
    "finactextot",
    "fintrntot",
    "fintrnextot",
    "fintxbtot",
    "fintxbextot",
    "fintaxclass",

    # Tentative assessment values: 5
    "tenmkttot",
    "tenacttot",
    "tentxbtot",
    "tentxbextot",
    "tentaxclass",

    # Special cases and tax status: 7
    "valref",
    "noav",
    "newdrop",
    "condo_number",
    "coop_num",
    "fintaxflag",
    "tentaxflag",

    # Protest indicators: 2
    "protest_1",
    "protest_2",
]

assert len(KEEP_COLS) == 55
assert len(set(KEEP_COLS)) == 55
