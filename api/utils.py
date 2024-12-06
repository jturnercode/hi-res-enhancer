import os
from datetime import datetime
import polars as pl


def format_dt(dt: datetime) -> str:
    """Return date and hr in filename format form

    Args:
        dt (datetime): datetime

    Returns:
        str: date & hour in hires filename format
    """
    date = str(dt.date()).replace("-", "_")

    hr = dt.hour * 100
    if hr == 0:
        hr = "0000"
    elif hr < 1000:
        hr = "0" + str(hr)
    else:
        hr = str(hr)

    return date, hr


def filter_directory(locid: str, sdt: datetime, edt: datetime):

    try:
        # return locid in filename format
        locid = ((5 - len(locid)) * "0") + locid

        # locate directory with files and create list
        path = os.getenv("DIRECTORY") + "Ctrl" + locid
        dir_list = os.listdir(path)

        # format datetime to filename date & hr format
        sdate, stime = format_dt(sdt)
        edate, etime = format_dt(edt)

        start_file_name = f"TRAF_{locid}_{sdate}_{stime}.csv"
        idx = dir_list.index(start_file_name)

        end_file_name = f"TRAF_{locid}_{edate}_{etime}.csv"
        idx_end = dir_list.index(end_file_name)

        # **Add hr if minute in end datetime
        if edt.minute > 0:
            idx_end += 1
        # print(idx, idx_end)

        dir_list = dir_list[idx:idx_end]

    # Typ error if file not found in directory, return empty list
    except Exception as err:
        print(err)
        return [], path
    return dir_list, path


def clean_csvs(dir_list: list, path: str):

    # ===========================
    #      Read Csv Data
    #   Clean/Format data
    #   Create one df from selected files
    # ===========================

    df_holder = []

    for file in dir_list:
        print(file)
        df = pl.read_csv(
            # source=path + "\\" + file,
            source=path + "/" + file,
            has_header=False,
            skip_rows=6,
            new_columns=["dt", "event_code", "parameter"],
        )

        # format columns
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

    return pl.concat(df_holder).sort(by="dt")


def pair_events(ec_pairs: list[tuple], df_data: pl.DataFrame) -> list[pl.DataFrame]:
    """Process events that have different event codes that mark start and finish of event.
    ex. Phase Green (ec=1, ec=7). The parameter determines phase for example case

    Args:
        ec_pairs (list[tuple]): [(event_start_code, event_end_code, event_descriptor), ...]
        df_data (pl.DataFrame): _description_

    Returns:
        list[pl.DataFrame]: _description_
    """

    eventdf_holder = []

    for ec_pair in ec_pairs:

        # Return series of all unigue parameters codes for current event_start code
        ec_params = df_data.filter(pl.col("event_code") == ec_pair[0])[
            "parameter"
        ].unique()

        for param in ec_params:
            # print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")

            df_ec = df_data.filter(
                pl.col("event_code").is_in([ec_pair[0], ec_pair[1]]),
                pl.col("parameter") == param,
            )
            # df_ec.write_csv("df_ec1.csv")

            # Shift event codes to compare
            # filter and keep event_codes that DO NOT MATCH (on/off pattern)
            df_ec = df_ec.filter(
                pl.col("event_code")
                != pl.col("event_code").shift(1, fill_value=ec_pair[1])
            )

            # df_ec.write_csv("df_ec2.csv")
            # print(df_ec)

            # Delete last row if ends with start pair
            if df_ec["event_code"].item(df_ec.height - 1) == ec_pair[0]:
                df_ec = df_ec.slice(0, df_ec.height - 1)

            # Separate all start event codes
            df_start = df_ec.filter(
                pl.col("event_code") == ec_pair[0], pl.col("parameter") == param
            )

            # Separate all end event codes
            df_end = df_ec.filter(
                pl.col("event_code") == ec_pair[1], pl.col("parameter") == param
            ).rename(lambda cname: cname + "2")

            # If Event does not have pair, skip without matching
            if df_start.is_empty() or df_end.is_empty():
                print("----- event codes with no matches -----")
                print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")
                continue

            # Stack horizontally
            df_temp = (
                df_start.hstack(df_end)
                .with_columns(duration=pl.col("dt2") - pl.col("dt"))
                .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
            )

            eventdf_holder.append(df_temp)

    return eventdf_holder


def single_events(ec_singles: list, df_data: pl.DataFrame) -> pl.DataFrame:
    """Process event codes that do not have end. These are just points in time and are notifications.
    ex. Coord Pattern Change

    Args:
        ec_singles (list): _description_
        df_data (pl.DataFrame): _description_

    Returns:
        pl.DataFrame: _description_
    """

    df_es = df_data.filter(pl.col("event_code").is_in(ec_singles))
    df_es2 = df_es.rename(lambda cname: cname + "2")

    df_singles = (
        df_es.hstack(df_es2)
        # added .1 seconds to be able to display on timeline chart, for now leave off
        # .with_columns(pl.col("dt2") + pl.duration(milliseconds=100))
        .with_columns(duration=pl.col("dt2") - pl.col("dt")).with_columns(
            pl.col("duration").dt.total_milliseconds() / 1000
        )
    )
    # df_temp.write_csv("temp.csv")

    return df_singles


def singles_wparams(df_ecodes: pl.DataFrame, df_data: pl.DataFrame) -> pl.DataFrame:
    """Process event codes that change depending on parameters.
    # ex. Unit Flash - Preempt (173,8); Unit Flash - MMU (173,6)

    Args:
        df_ecodes (pl.DataFrame): event codes read from csv
        df_data (pl.DataFrame): dataset read from purdue csv file

    Returns:
        pl.DataFrame: processed result that will be added to final df
    """

    # Temp column to match event/parameter to event/parameter in data
    df_ecodes = df_ecodes.with_columns(
        temp=pl.col("event_code").cast(pl.String)
        + pl.lit("-")
        + pl.col("event_param").cast(pl.String)
    )

    df_es = (
        df_data.filter(pl.col("event_code").is_in(df_ecodes["event_code"].unique()))
        .with_columns(
            temp=pl.col("event_code").cast(pl.String)
            + pl.lit("-")
            + pl.col("parameter").cast(pl.String)
        )
        .with_columns(
            event_descriptor=pl.col("temp").replace_strict(
                old=df_ecodes["temp"],
                new=df_ecodes["event_description"],
                default="unknown?",
            )
        )
        .drop("temp")
    )

    df_es2 = df_es.rename(lambda cname: cname + "2")

    df_singles_wparams = (
        df_es.hstack(df_es2)
        .with_columns(duration=pl.col("dt2") - pl.col("dt"))
        .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
    )

    return df_singles_wparams
