import matplotlib.pyplot as plt
import pickle
import numpy as np

if __name__ == "__main__":
    # read pickle of start time
    with open("client.pickle", "rb") as file:
        map_ts_begin = pickle.load(file)
    map_ts_begin = {k: float(v) for k, v in map_ts_begin.items()}
    print(f"map_ts_begin: {map_ts_begin}")

    # read pickle of complete time
    with open("server.pickle", "rb") as file:
        map_ts_complete = pickle.load(file)
    map_ts_complete = {k: float(v) for k, v in map_ts_complete.items()}
    print(f"map_ts_complete: {map_ts_complete}")

    # load filename and filesize
    map_filename_to_filesize = {}
    with open("./config_client", 'r') as clientConfig:
        for line in clientConfig.readlines():
            if line[0] == "#":
                continue
            parsed_line = line.replace('\n', '').split(
                ',')  # time, filename, filesize
            assert(len(parsed_line) == 3)
            map_filename_to_filesize[parsed_line[1]] = parsed_line[2]
    print(f"map_filename_to_filesize: {map_filename_to_filesize}")

    # sanity check
    assert(set(map_ts_begin.keys()) == set(map_ts_complete.keys()))

    # plot histogram and save
    list_tsdiff = []
    for filename in map_ts_begin.keys():
        list_tsdiff.append([map_filename_to_filesize[filename],
                                        round(2 * (map_ts_complete[filename] - map_ts_begin[filename]), 1)/2.0])
    print(f"list_tsdiff: {list_tsdiff}")
    plt.hist([v[1] for v in list_tsdiff])
    plt.savefig('histogram.png')

    print("\n\n*** Average completion time : {0:2.4f}".format(np.mean([v[1] for v in list_tsdiff])))