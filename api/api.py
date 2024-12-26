import io, os, math
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import polars as pl
import utils

uri = os.getenv("URI")
app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    # allow_origins=["http://127.0.0.1:5500"],
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================================
#                Test Endpoint
# ================================================


@app.get("/")
async def root():
    return {"message": "Hello, Root!"}


# ===========================
#  * Load Event code data
# ===========================
# TODO: save codes to db to deploy on server?? works fine with csv.
# event codes with descriptions
ec = pl.read_csv(source="api/event_codes.csv")

# event code pairs
exclude = [200, 82]
ec_pairs: pl.DataFrame = (
    pl.read_csv("api/event_pairs.csv")
    # some event codes have multiple event_end codes (ex. overlap red for FYA)
    # made event_end into list to accomodate multiple values
    .with_columns(
        event_end=pl.col("event_end").str.split(by="-").cast(pl.List(pl.Int32))
    ).filter(~pl.col("event_start").is_in(exclude))
)

# single event codes
ec_singles = pl.read_csv("api/event_singles.csv")["event_code"].to_list()

# single event codes with parameters
# ex. Unit Flash - Preempt (173,8); Unit Flash - MMU (173,6)
ec_single_wparams: pl.DataFrame = pl.read_csv("api/ec_singles_wParams.csv")

# ================================================
# *             Locations
# Used to populate locations dropdown
# ================================================


@app.get("/form_locids")
async def get_locations() -> list[dict]:
    """Return list of atms_id, location name dictionaries
    for select dropdown"""

    qry = """SELECT atms_id, name FROM intersection"""
    df = pl.read_database_uri(query=qry, uri=uri, engine="adbc").sort("name")

    # NOTE: anotther way to acheive results
    # w/o list, only dictionary
    # df = dict(zip(df["atms_id"], df["name"]))
    return df.to_dicts()


# ================================================
# *               Hi-res Function
# get and process hi-res data into dataframe
# ================================================


def process_hires(locid: str, sdate: datetime, edate: datetime) -> pl.DataFrame:

    # return filtered list of files from directory
    dir_list, path = utils.filter_directory(locid, sdate, edate)
    print(dir_list)

    if not dir_list:
        return pl.DataFrame()

    # Read, clean, and concat csv files
    df_data: pl.DataFrame = utils.clean_csvs(dir_list, path)

    # Use series to map values from df to another df, great feature!!
    df_data = df_data.with_columns(
        event_descriptor=pl.col("event_code").replace_strict(
            old=ec["event_code"], new=ec["event_descriptor"], default="unknown?"
        )
    ).filter(
        # Filter out events that start between sdate & edate
        pl.col("dt").is_between(sdate, edate),
        # REMOVE DETECTOR EVENTS
        ~pl.col("event_code").is_in([81, 82]),
    )

    # TODO: get all unique event codes to iterate less?

    print("----** Start data size: ", df_data.shape)

    # ================================================
    #  *        Single Event Codes w/Parmameters
    #   Modify Events code descriptors that change with
    #  parameter number
    # ================================================

    df_data = utils.singles_wparams(ec_single_wparams, df_data)

    # ================================================
    # *             Event Code Intervals
    #  Intervals for events that have on/off via event_code
    # & parameters are the same.
    # Need intervals to show different status in ag-grid
    # (phase, overlap, & other operation status)
    # ================================================
    nfPeriods, fPeriods = utils.flash_periods(df_data, sdate, edate)
    nonFlashEventInts: pl.DataFrame = utils.event_intervals(
        ec_pairs, df_data, nfPeriods
    )

    # Add abbr column to non Flash interval results
    # map abbr from ec_pairs to event_code col in nonFlashEventInts df
    nonFlashEventInts = nonFlashEventInts.with_columns(
        abbr=pl.col("event_code").replace_strict(
            old=ec_pairs["event_start"], new=ec_pairs["abbr"]
        )
    )
    print(nonFlashEventInts)

    # Add status columns as list type to df_data
    df_data = df_data.with_columns(
        phase_status=[], ovl_status=[], ops_status=[], time_grp=None
    )

    # ================================================
    # *             Add Phase Status Info
    #  ???
    # ================================================

    phase_int = nonFlashEventInts.filter(pl.col("event_code").is_in([1, 8, 10, 21, 22]))

    for int in phase_int.to_dicts():

        df_data = df_data.with_columns(
            phase_status=pl.when(
                pl.col("dt").is_between(int["dt"], int["dt2"], closed="left")
            )
            .then(
                pl.col("phase_status").list.concat(
                    [str(int["parameter"]) + pl.lit(int["abbr"])]
                )
            )
            .otherwise(pl.col("phase_status"))
        )

    # ================================================
    # *             Add Overlap Status Info
    #  ???
    # ================================================

    phase_int = nonFlashEventInts.filter(
        pl.col("event_code").is_in([61, 63, 64, 62, 67, 68])
    )

    for int in phase_int.to_dicts():

        df_data = df_data.with_columns(
            ovl_status=pl.when(
                pl.col("dt").is_between(int["dt"], int["dt2"], closed="left")
            )
            .then(
                pl.col("ovl_status").list.concat(
                    [str(int["parameter"]) + pl.lit(int["abbr"])]
                )
            )
            .otherwise(pl.col("ovl_status"))
        )

    # ================================================
    # *             Add Operational Status Info
    #  ???
    # ================================================

    phase_int = nonFlashEventInts.filter(
        pl.col("event_code").is_in([32, 102, 105, 106, 107, 111, 182])
    )

    for interval in phase_int.to_dicts():

        df_data = df_data.with_columns(
            ops_status=pl.when(
                pl.col("dt").is_between(interval["dt"], interval["dt2"], closed="left")
            )
            .then(
                pl.col("ops_status").list.concat(
                    [str(interval["parameter"]) + pl.lit(interval["abbr"])]
                )
            )
            .otherwise(pl.col("ops_status"))
        )

    # =================== Single Event Ops Code ================
    # Events that only have a start but no end codes
    se = [(111, "PreExit")]
    # TODO: Remove hard coded events from here and add to file or db
    # TODO: set all events at detected event time vs just one event
    for e in se:
        df_data = df_data.with_columns(
            ops_status=pl.when(pl.col("event_code") == e[0])
            .then(pl.col("ops_status").list.concat(pl.lit(e[1])))
            .otherwise(pl.col("ops_status"))
        )

    # ============= Add Flash events to ops status ==============
    for int in fPeriods:

        df_data = df_data.with_columns(
            ops_status=pl.when(pl.col("dt").is_between(int[0], int[1], closed="left"))
            .then(pl.col("ops_status").list.concat(pl.lit("Flash")))
            .otherwise(pl.col("ops_status"))
        )

    # ================================================
    #                Group Events by DT
    # Ag grid will highlight to be able to quickly distinguish
    # time change
    # ================================================

    time_grp = df_data.select(pl.col("dt").unique().sort()).to_series().to_list()

    ints = []
    i = 0
    while i < len(time_grp) - 1:
        ints.append((time_grp[i], time_grp[i + 1]))
        i += 2

    for i in ints:
        df_data = df_data.with_columns(
            time_grp=pl.when(pl.col("dt").is_between(i[0], i[1], closed="left"))
            .then(pl.lit("x"))
            .otherwise(pl.col("time_grp"))
        )

    # ================================================
    #     Add locid column and sort/formatting
    # ================================================
    df_data = (
        df_data.sort(by=["dt", "event_code"]).select(
            pl.lit(locid).alias("loc_id"),
            pl.all(),
        )
        # Format dates to string and round off duration
        .with_columns(
            pl.col("dt").dt.strftime(r"%Y-%m-%d %H:%M:%S%.3f"),
        )
    )

    # ? Test code
    # df_data = df_data.filter(
    #     pl.col("event_code").is_in([200, 201, 173]),
    #     pl.col("parameter").is_in([15, 5, 6, 7, 2, 3, 8, 1]),
    # )

    # TODO: calculate time between previous event shown in grid; helps review of events
    return df_data


# ================================================
# *            Hi-res AG grid endpoint
# Populate hi-res data in ag grid
# ================================================

# Test Link
# http://127.0.0.1:5500/agrid_index.html?startdt=2024-12-04T15:40&enddt=2024-12-04T15:55&locid=601


@app.get("/hiresgrid")
async def get_hires_grid(
    locid: str,
    startdt: str,
    enddt: str,
    # time: str | None = "0000",
    # addhrs: str | None = "1",
) -> list[dict]:

    enddt = datetime.fromisoformat(enddt)
    startdt = datetime.fromisoformat(startdt)

    numberOfHrs = (enddt - startdt).total_seconds() // (3600)

    if numberOfHrs > 24:
        print("Too much data")
        # TODO: return message that to much data requested
        return pl.DataFrame().to_dicts()

    df_hres = process_hires(locid=locid, sdate=startdt, edate=enddt)
    # print(df_hres.columns)

    return df_hres.to_dicts()


# ================================================
# *        Download Processed HiRes Csv
# ================================================


@app.get("/purdue")
async def get_purdue(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> StreamingResponse:

    df_hres: pl.DataFrame = process_hires(
        locid=locid, date=date, time=time, addhrs=addhrs
    )

    stream = io.StringIO()
    df_hres.write_csv(stream)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


# ================================================
# *        Endpoint for HiRes Timeline
# ================================================


@app.get("/timeline_viz")
async def get_purdue(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> dict:

    df_hres = process_hires(locid=locid, date=date, time=time, addhrs=addhrs)

    df_viz = df_hres.with_columns(
        pl.col("dt").dt.timestamp("ms"), pl.col("dt2").dt.timestamp("ms")
    )

    series = []
    colors = []

    ring_ec = {
        "Green": (1, 7, "#00E396"),
        "Yellow Clr": (8, 9, "#FEB019"),
        "Red Clr": (10, 11, "#FF4560"),
    }
    # TODO: how do i get this info for each intersection as ring structures vary?
    phases = [
        (1, "R1"),
        (2, "R1"),
        (3, "R1"),
        (4, "R1"),
        (5, "R2"),
        (6, "R2"),
        (7, "R2"),
        (8, "R2"),
    ]
    num_records = 0
    for k, v in ring_ec.items():

        # TODO: how do i add the phase here to name without another nested for loop?
        for phase in phases:
            df = df_viz.filter(
                (pl.col("event_code") == v[0])
                & (pl.col("event_code2") == v[1])
                & (pl.col("parameter") == phase[0])
            )
            # df.write_csv("api/test_results.csv")

            df = df.with_columns(
                y=pl.concat_list("dt", "dt2"), x=pl.lit(phase[1])
            ).select(["x", "y"])

            # df.write_json("api/test_results.json")

            temp_d = {"name": f"Ph {phase[0]} {k}"}
            num_records += len(df)
            temp_d["data"] = df.to_dicts()
            series.append(temp_d)
            colors.append(v[2])

    # with open("api/res.json", "w") as f:
    #     for items in series:
    #         f.write("%s\n" % items)
    print(num_records)

    res = dict()
    res["series"] = series
    res["colors"] = colors
    return res
