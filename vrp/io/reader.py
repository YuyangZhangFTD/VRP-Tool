import pandas as pd

from vrp.common.model import SeqDict
from vrp.util.func import cal_euclidean_distance, cal_manhattan_distance


# TODO
def read_data(data_set=None, **kwargs):
    if data_set == "soloman":
        return _read_data_soloman
    elif data_set == "homberger":
        pass
    elif data_set == "lim":
        pass
    elif data_set == "joc":
        pass
    else:
        raise FileNotFoundError("Data set is not found.")


def _read_data_soloman(**kwargs):
    name = kwargs.get("name", "c101")
    dist_metric = kwargs.get("distance_metric", "Euclidean")
    if dist_metric == "Euclidean":
        cal_dist = cal_euclidean_distance
    elif dist_metric == "Manhattan":
        cal_dist = cal_manhattan_distance
    else:
        raise Exception(
            "Distance metric " + dist_metric + " is not implemented"
        )

    file_name = name + ".txt"
    f = open("Soloman-Data/" + file_name)
    for _ in range(4):
        f.readline()
    number, *_, capacity = f.readline().strip().split(" ")
    header = [
        "CustomerNumber",
        "XCoordinate",
        "YCoordinate",
        "Demand",
        "ReadyTime",
        "DueTime",
        "ServiceTime"
    ]
    for _ in range(4):
        f.readline()
    pd_list = list()
    while True:
        tmp = f.readline()
        if len(tmp) < 2:
            break
        pd_list.append([int(x.strip()) for x in tmp.split("      ")])
    f.close()
    df = pd.DataFrame(pd_list, columns=header)
    # TODO: handle df
    return None


def _time_transformer(s):
    a = s.split(":")
    return int(a[0]) * 60 + int(a[1]) - 8 * 60


def read_data_goc(number):
    if number == 1:
        dt = pd.read_csv("input_B/inputdistancetime_1_1601.txt")
        node = pd.read_csv("input_B/inputnode_1_1601.csv", sep="\t")
    elif number == 2:
        dt = pd.read_csv("input_B/inputdistancetime_2_1501.txt")
        node = pd.read_csv("input_B/inputnode_2_1501.csv", sep="\t")
    elif number == 3:
        dt = pd.read_csv("input_B/inputdistancetime_3_1401.txt")
        node = pd.read_csv("input_B/inputnode_3_1401.csv", sep="\t")
    elif number == 4:
        dt = pd.read_csv("input_B/inputdistancetime_4_1301.txt")
        node = pd.read_csv("input_B/inputnode_4_1301.csv", sep="\t")
    elif number == 5:
        dt = pd.read_csv("input_B/inputdistancetime_5_1201.txt")
        node = pd.read_csv("input_B/inputnode_5_1201.csv", sep="\t")
    else:
        return None

    from_to_node = list(zip(dt["from_node"].values, dt["to_node"].values))
    ds = SeqDict({
        ((k1,), (k2,)): v
        for (k1, k2), v in pd.Series(
        dt["distance"].values, index=from_to_node
    ).items()
    })
    tm = SeqDict({
        ((k1,), (k2,)): v
        for (k1, k2), v in pd.Series(
        dt["spend_tm"].values, index=from_to_node
    ).items()
    })
    del dt

    node.columns = [
        "ID",
        "type",
        "lng",
        "lat",
        "weight",
        "volume",
        "first",
        "last"
    ]

    node["first"] = node.loc[:, "first"].apply(
        lambda x: _time_transformer(x) if x != "-" else 0
    )
    node["last"] = node.loc[:, "last"].apply(
        lambda x: _time_transformer(x) if x != "-" else 960
    )

    delivery = node[node.type == 2]
    pickup = node[node.type == 3]
    charge = node[node.type == 4]

    lng_lat = list(zip(node["lng"].values, node["lat"].values))
    position = pd.Series(lng_lat, index=node["ID"]).to_dict()

    del node

    delivery_range = [delivery["ID"].min(), delivery["ID"].max()]
    pickup_range = [pickup["ID"].min(), pickup["ID"].max()]
    charge_range = [charge["ID"].min(), charge["ID"].max()]
    return ds, tm, delivery, pickup, charge, position, \
           [
               lambda x:
               True if delivery_range[0] <= x <= delivery_range[1] else False,
               lambda x:
               True if pickup_range[0] <= x <= pickup_range[1] else False,
               lambda x:
               True if charge_range[0] <= x <= charge_range[1] else False
           ]


def get_node_info(node, is_charge=False):
    node_id = {(x,) for x in node["ID"].values.tolist()}
    if is_charge:
        # weight = {
        #     (k,): 0
        #     for k, v in pd.Series(
        #         node["weight"].values, index=node["ID"].values
        #     ).items()
        # }
        # volume = {
        #     (k,): 0
        #     for k, v in pd.Series(
        #         node["volume"].values, index=node["ID"].values
        #     ).items()
        # }
        volume, weight = None, None
        first = {
            (k,): 0
            for k, v in pd.Series(
            node["first"].values, index=node["ID"].values
        ).items()
        }
        last = {
            (k,): 960
            for k, v in pd.Series(
            node["last"].values, index=node["ID"].values
        ).items()
        }
    else:
        weight = {
            (k,): float(v)
            for k, v in pd.Series(
            node["weight"].values, index=node["ID"].values
        ).items()
        }
        volume = {
            (k,): float(v)
            for k, v in pd.Series(
            node["volume"].values, index=node["ID"].values
        ).items()
        }
        first = {
            (k,): int(v)
            for k, v in pd.Series(
            node["first"].values, index=node["ID"].values
        ).items()
        }
        last = {
            (k,): int(v)
            for k, v in pd.Series(
            node["last"].values, index=node["ID"].values
        ).items()
        }
    del node
    return node_id, volume, weight, first, last
