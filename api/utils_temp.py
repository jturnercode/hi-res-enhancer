import polars as pl


#! TODO: NOT USED, DO I NEED FOR DURATION TYPE VIEW
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


#! TODO: NOT USED, DO I NEED FOR DURATION TYPE VIEW
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
