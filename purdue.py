import polars as pl

# event codes
ec = pl.read_csv(source="event_codes.csv")


# ===========================
#      Read Csv Data
#
#
# ===========================
# purdue data in csv
# TODO: add code to read as csv in directory
# TODO: add intersection id to results

df = pl.read_csv(
    source="TRAF_00608_2024_08_23_0600.csv",
    has_header=False,
    skip_rows=6,
    new_columns=["dt", "event_code", "parameter"],
)


# ===========================
#      Clean/Format data
#
#   add event descriptor names
# ===========================

df = (
    df.with_columns(
        pl.col("dt").str.to_datetime(r"%-m/%d/%Y %H:%M:%S%.3f"),
        pl.col("event_code").str.replace_all(" ", ""),
        pl.col("parameter").str.replace_all(" ", ""),
    ).with_columns(
        pl.col("event_code").str.to_integer(),
        pl.col("parameter").str.to_integer(),
    )
    # Use series to map values from df to another df, great feature!!
    .with_columns(
        event_descriptor=pl.col("event_code").replace_strict(
            old=ec["event_code"], new=ec["event_descriptor"], default="x"
        )
    )
)


# ====================================
#       Split Duration Calculation
#
#  begin green to Phase end redclear
# ====================================

df_holder = []

phases = df.filter(pl.col("event_code").is_in([1]))["parameter"].unique()

# test code
# phases = [6]

for phase in phases:

    df_start = df.filter(pl.col("event_code").is_in([1]), pl.col("parameter") == phase)
    df_end = df.filter(
        pl.col("event_code").is_in([11]), pl.col("parameter") == phase
    ).rename(lambda cname: cname + "2")

    # delete first row of df_end of start time > end time
    if df_start["dt"].item(0) > df_end["dt2"].item(0):
        df_end = df_end.slice(1, df_end.height)

    # delete last row of df_start if start time > end time
    if df_start["dt"].item(df_start.height - 1) > df_end["dt2"].item(df_end.height - 1):
        df_start = df_start.slice(0, df_start.height - 1)

    if df_end.height > df_start.height:
        print("here 1")
        df_end = df_end.slice(1, df_start.height)
    elif df_start.height > df_end.height:
        print("here 2")
        df_start = df_start.slice(0, df_end.height)

    # print(df_start)
    # print(df_end)
    # quit()

    df_temp = (
        df_start.hstack(df_end)
        .with_columns(duration=pl.col("dt2") - pl.col("dt"))
        .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
    )

    df_holder.append(df_temp)

df_fin: pl.DataFrame = pl.concat(df_holder)
df_fin.sort(by="dt").write_csv("results.csv")
