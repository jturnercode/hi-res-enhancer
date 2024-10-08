import os
import polars as pl


def filter_directory(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
):

    try:
        locid = ((5 - len(locid)) * "0") + locid

        # locate directory with files and create list
        path = os.getenv("DIRECTORY") + "Ctrl" + locid
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

    except:
        return [], path
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
            # source=path + "\\" + file,
            source=path + "/" + file,
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


def pair_events(ec_pairs, df_data) -> list[pl.DataFrame]:

    eventdf_holder = []

    for ec_pair in ec_pairs:

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

            df_ec = df_ec.filter(
                pl.col("event_code")
                != pl.col("event_code").shift(1, fill_value=ec_pair[1])
            )

            # df_ec.write_csv("df_ec2.csv")
            # print(df_ec)

            # Delete last row if ends with start pair
            if df_ec["event_code"].item(df_ec.height - 1) == ec_pair[0]:
                df_ec = df_ec.slice(0, df_ec.height - 1)

            df_start = df_ec.filter(
                pl.col("event_code") == ec_pair[0], pl.col("parameter") == param
            )

            df_end = df_ec.filter(
                pl.col("event_code") == ec_pair[1], pl.col("parameter") == param
            ).rename(lambda cname: cname + "2")

            # If Event does not have pair, skip without matching
            if df_start.is_empty() or df_end.is_empty():
                print(f"ec1: {ec_pair[0]}, ec2: {ec_pair[1]}: param: {param} ")
                continue

            # print(df_start)
            # print(df_end)

            df_temp = (
                df_start.hstack(df_end)
                .with_columns(duration=pl.col("dt2") - pl.col("dt"))
                .with_columns(pl.col("duration").dt.total_milliseconds() / 1000)
            )

            eventdf_holder.append(df_temp)

    return eventdf_holder


def single_events(ec_singles: list, df_data) -> pl.DataFrame:

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
