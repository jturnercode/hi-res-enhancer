import os
import polars as pl


def filter_directory(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
):

    locid = ((5 - len(locid)) * "0") + locid

    # locate directory with files and create list
    path = os.getenv("DIRECTORY") + locid
    dir_list = os.listdir(path)

    # format date to format in filename
    date = date.replace("-", "_")
    date = date.replace("/", "_")

    if time == None:
        time = "0000"

    file_name = f"TRAF_{locid}_{date}_{time}.csv"
    idx = dir_list.index(file_name)

    if addhrs == None:
        idx_end = None
    else:
        idx_end = idx + int(addhrs)

    dir_list = dir_list[idx:idx_end]
    return dir_list, path


def add_event_descriptors(dir_list: list, path: str):

    # ===========================
    #      Read Csv Data
    #
    #   Create one df from selected files
    #   Add event descriptor to each event
    # ===========================

    df_holder = []

    for file in dir_list:
        print(file)
        df = pl.read_csv(
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

    return df_holder
