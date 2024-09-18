import io
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import polars as pl
import utils

app = FastAPI()


# ===========================
#  Read event code descriptors
#
#
# ===========================

# event codes
ec = pl.read_csv(source="event_codes.csv")


@app.get("/purdue")
async def get_purdue(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
) -> StreamingResponse:

    # retun filtered list of files from directory
    dir_list, path = utils.filter_directory(locid, date, time, addhrs)

    # read files and add event descriptors
    df_holder: list[pl.DataFrame] = utils.add_event_descriptors(dir_list, path)

    # Concat list of dataframes to one dataframe for processing
    df_data: pl.DataFrame = (
        pl.concat(df_holder).sort(by="dt")
        # Use series to map values from df to another df, great feature!!
        .with_columns(
            event_descriptor=pl.col("event_code").replace_strict(
                old=ec["event_code"], new=ec["event_descriptor"], default="x"
            )
        )
    )

    # ================================================
    #            Paired Event Code
    #
    #  alarms that have seperate event codes for on/off
    #
    # ================================================

    ec_pairs = pl.read_csv("event_pairs.csv").rows()
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

    # ================================================
    #                Single Event Code
    #
    #
    #   Events that only have singel event code
    #
    # ================================================

    ec_singles = pl.read_csv("event_singles.csv")
    ec_singles = ec_singles["event_code"].to_list()

    # for ec_single in ec_singles:

    #     print(f"ec1: {ec_single[0]}, descriptor: {ec_single[1]} ")

    df_es = df_data.filter(pl.col("event_code").is_in(ec_singles))
    df_es2 = df_es.rename(lambda cname: cname + "2")

    df_temp = (
        df_es.hstack(df_es2)
        # added .1 seconds to be able to display on timeline chart, for now leave off
        # .with_columns(pl.col("dt2") + pl.duration(milliseconds=100))
        .with_columns(duration=pl.col("dt2") - pl.col("dt")).with_columns(
            pl.col("duration").dt.total_milliseconds() / 1000
        )
    )
    # df_temp.write_csv("temp.csv")

    eventdf_holder.append(df_temp)

    df_fin: pl.DataFrame = (
        pl.concat(eventdf_holder)
        .sort(by="dt")
        .select(pl.lit(locid).alias("loc_id"), pl.all())
    )

    stream = io.StringIO()
    df_fin.write_csv(stream)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response
