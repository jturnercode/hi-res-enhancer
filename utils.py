import os


def filter_directory(
    locid: str, date: str, time: str | None = None, addhrs: int | None = None
):

    locid = ((5 - len(locid)) * "0") + locid

    # locate directory with files and create list
    path = os.getenv("DIRECTORY") + locid
    dir_list = os.listdir(path)

    if date != None:
        date = date.replace("-", "_")

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
