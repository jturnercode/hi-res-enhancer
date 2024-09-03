import os
import polars as pl
import env


# ===========================
#  Read event code descriptors
#
#
# ===========================

# event codes
ec = pl.read_csv(source="event_codes.csv")


# ===========================
#      User Input
#
#   locid, date, time, # hours
# ===========================

id_input = input("ENTER Intersection ID: ")
locid = ((5 - len(id_input)) * "0") + id_input

# locate directory with files and create list
path = env.DIRECTORY + locid
dir_list = os.listdir(path)
# print(dir_list)

# TODO: add options to just look at all files
id_date = input("Enter date to process (ex. 2024-08-04): ")
id_date = id_date.replace("-", "_")

id_hour = input("Enter start time (ex. 1400): ")
add_hours = input("Enter number of hours after start time: ")

file_name = f"TRAF_{locid}_{id_date}_{id_hour}.csv"
indx = dir_list.index(file_name)

dir_list = dir_list[indx : indx + int(add_hours)]


# ===========================
#      Process Csv Data
#
#
# ===========================

df_holder = []


for file in dir_list:
    print(file)
    df = pl.read_csv(
        # source="TRAF_00151_2024_08_22_0600.csv",
        source=path + "\\" + file,
        has_header=False,
        skip_rows=6,
        new_columns=["dt", "event_code", "parameter"],
    )

    # ===========================
    #      Clean/Format data
    #
    #   add event descriptor names
    # ===========================

    df = df.with_columns(
        pl.col("dt").str.to_datetime(r"%-m/%d/%Y %H:%M:%S%.3f"),
        pl.col("event_code").str.replace_all(" ", ""),
        pl.col("parameter").str.replace_all(" ", ""),
        # location_id=pl.lit(locid),
    ).with_columns(
        pl.col("event_code").str.to_integer(),
        pl.col("parameter").str.to_integer(),
    )

    df_holder.append(df)

# Concat all data
df_data: pl.DataFrame = (
    pl.concat(df_holder).sort(by="dt")
    # Use series to map values from df to another df, great feature!!
    .with_columns(
        event_descriptor=pl.col("event_code").replace_strict(
            old=ec["event_code"], new=ec["event_descriptor"], default="x"
        )
    )
)
# print(df_data)


# ====================================
#       Split Duration Calculation
#
#  begin green to Phase end redclear
# ====================================

phases = df_data.filter(pl.col("event_code").is_in([1]))["parameter"].unique()

# test code
# phases = [6]

df_holder.clear()

for phase in phases:
    # print(phase)

    df_start = df_data.filter(
        pl.col("event_code").is_in([1]), pl.col("parameter") == phase
    )
    df_end = df_data.filter(
        pl.col("event_code").is_in([11]), pl.col("parameter") == phase
    ).rename(lambda cname: cname + "2")

    # TODO: handle df_start empty or df_end empty

    # Delete first row of df_end of start time > end time
    if df_start["dt"].item(0) > df_end["dt2"].item(0):
        df_end = df_end.slice(1, df_end.height)

    # Delete last row of df_start if start time > end time
    if df_start["dt"].item(df_start.height - 1) > df_end["dt2"].item(df_end.height - 1):
        df_start = df_start.slice(0, df_start.height - 1)

    # TODO: how does below still occur with above logic?
    # Below handles an event starting not ending before hour ends and event not ending from previous hour
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


df_fin: pl.DataFrame = pl.concat(df_holder).sort(by="dt")
df_fin.write_csv("results.csv")
