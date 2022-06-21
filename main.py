import srcomapi
import srcomapi.datatypes as dt
import json
import sqlite3
import pandas as pd
import argparse
import requests_cache
from tenacity import *
import os
import errno
import hashlib
import requests
import os.path

parser = argparse.ArgumentParser(
    "Dump speedrun.com leaderboard to JSON/CSV/SQLite")
parser.add_argument('game', metavar='G', nargs=None, help='id of game')
parser.add_argument('category', metavar='C',
                    nargs=None, help='name of category')
parser.add_argument(
    "-il", nargs="?", help='name of IL for individual level categories', required=False)
parser.add_argument('--json', action='store_true', help="output JSON")
parser.add_argument('--csv', action='store_true', help="output CSV")
parser.add_argument('--sqlite', action='store_true', help="output SQLite DB")

args = parser.parse_args()

requests_cache.install_cache()

api = srcomapi.SpeedrunCom()
api.debug = 1


# @retry(wait=wait_exponential(multiplier=1, min=4, max=20))
def get_all_runs(gameid, catid):
    ret = list()
    i = 1
    max = 200
    orderby = "submitted"

    directions = ["desc", "asc"]

    for dir in directions:
        print(len(ret))
        uri = "https://www.speedrun.com/api/v1/runs?game={}&category={}&max={}&embed=players%2Cplatform%2Cregion&orderby={}&direction={}".format(
            gameid, catid, max, orderby, dir)

        while True:
            print(uri)
            print(i)
            i += 1
            req = requests.get(uri).json()

            if "status" in req and req["status"] == 400:
                break

            size = req["pagination"]["size"]
            if size > 0:
                ret = ret + req["data"]

            if size < max:
                break

            uri = next(filter(lambda l: l["rel"] == "next", req["pagination"]["links"]))[
                "uri"]

        if len(ret) < 10000:
            break
        
    return ret


# @retry(wait=wait_exponential(multiplier=1, min=4, max=20))
def get_game_leaderboards(game_id):
    game = api.get_game(game_id)

    lbs = {}

    for category in game.categories:
        if category.name != args.category:
            continue
        if not category.name in lbs:
            lbs[category.name] = {}
        if category.type == 'per-level' and args.il:
            for level in game.levels:
                lbs[category.name][level.name] = dt.Leaderboard(api, data=api.get(
                    "leaderboards/{}/level/{}/{}?embed=variables".format(game.id, level.id, category.id)))
        elif category.type == 'per-game' and not args.il:
            lbs[category.name] = get_all_runs(game.id, category.id)
            # "leaderboards/{}/category/{}?embed=variables".format(game.id, category.id)))

    return lbs


# @retry(wait=wait_exponential(multiplier=1, min=4, max=20))
def append_run(r, runs):
    subcategory = ""

    if r["values"]:
        subcategory = ", ".join(
            list(map(lambda k: dt.Variable(api, api.get("variables/{}".format(k))).values["values"][r["values"][k]]["label"], r["values"].keys())))

    videos = ""
    if not r["videos"] is None:
        videos = ", ".join(list(map(lambda v: v['uri'], r["videos"]['links']))) if r["videos"].get(
            "text") is None else r["videos"].get("text")

    players = ""
    try:
        players = ", ".join(
            list(map(lambda p: p['names']['international'], r["players"]["data"])))
    except:
        players = ", ".join(
            list(map(lambda p: p['name'], r["players"]["data"])))

    rejected = 1 if r["status"]["status"] == "rejected" else 0
    new = 1 if r["status"]["status"] == "new" else 0

    examiner = ""

    if "examiner" in r["status"] and not r["status"]["examiner"] is None:
        examiner_req = requests.get(
            "https://www.speedrun.com/api/v1/users/{}".format(r["status"]["examiner"])).json()
        if "status" not in examiner_req:
            examiner = examiner_req["data"]["names"]["international"]

    reason = r["status"]["reason"] if rejected else ""
    reason = '' if reason is None else str(reason)

    runs.append(
        {
            "subcategory": subcategory,
            "players": players,
            "times": r["times"]['primary_t'],
            "platform": r["platform"]["data"]["name"] if r["platform"]["data"] else "",
            "region": r["region"]["data"]["name"] if r["region"]["data"] else "",
            "emulated": r["system"]['emulated'],
            "date": r["date"],
            "comment": r["comment"],
            "videos": videos,
            "rejected": rejected,
            "reason": reason,
            "examiner": examiner,
            "new": new,
            "id": r["id"]
        }
    )


def get_runs_list(lb):
    runs = []
    i = 0
    for run in lb:
        print(i)
        append_run(run, runs)
        i = i + 1

    return runs


lbs = get_game_leaderboards(args.game)

runs = get_runs_list(
    lbs[args.category] if args.il is None else lbs[args.il][args.category])

runs_json = json.dumps(runs)

dir = 'out/{}-{}'.format(args.game, args.category)

try:
    os.makedirs("out")
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

if args.json:
    with open(dir + '.json', 'w') as f:
        f.write(runs_json)

if args.csv:
    with open(dir + '.csv', 'w') as f:
        f.write(pd.read_json(runs_json).to_csv())

header_dup = True
header_new = True

if args.sqlite:
    connection = sqlite3.connect('out/srcom.sqlite')
    cursor = connection.cursor()
    cursor.execute(
        'Create TABLE if not exists {} (category text, player text, time real, platform text, region text, emulated integer, date text, comment text, link text, rejected integer, reason text, examiner text, new integer, [editor\'s note] text, cheated integer, removed integer, disputed integer, anonymised integer, unsubmitted integer, [no video] integer, subcategory text, id text, hash text)'.format(args.game))

    #columns = list(runs[0].keys())
    columns = ["players", "times", "platform", "region", "emulated",
               "date", "comment", "videos", "rejected", "reason", "examiner", "new"]
    for row in runs:
        keys = (args.category,) + \
            tuple(row[c] for c in columns) + (None, False, False, False, False,
                                              False, True if row["videos"] == "" else False) + (row['subcategory'], row['id'])
        keys_hash = (args.category,) + \
            tuple(row[c] for c in columns) + (row['subcategory'], row['id'])
        hash = hashlib.sha256(repr(keys_hash).encode()).hexdigest()

        # cursor.execute("SELECT * FROM {} WHERE player='{}' AND category='{}' AND time='{}' AND date='{}' AND rejected='{}'"
        # .format(args.game, row["players"], args.category, row["times"], row["date"], row["rejected"], row["comment"], row["reason"]))
        cursor.execute("SELECT hash FROM {} WHERE id='{}'".format(
            args.game, row["id"]))
        result = cursor.fetchall()
        # print(result)
        if len(result) > 0:
            h = result[0][0]
            if h == hash:
                with open("out/duplicates.csv", 'a+') as f:
                    cursor.execute(
                        "SELECT * FROM {} WHERE id='{}'".format(args.game, row["id"]))
                    full_row = cursor.fetchall()
                    f.write(pd.read_json(json.dumps(full_row)).to_csv(
                        header=header_dup, index=False))
                    if header_dup == True:
                        header_dup = False
                continue
            else:
                cursor.execute("DELETE from {} where id='{}'".format(
                    args.game, row["id"]))

        with open("out/new.csv", 'a+') as f:
            f.write(pd.read_json(json.dumps(
                [(args.game,) + keys + (hash,)])).to_csv(header=header_new, index=False))
            if header_new == True:
                header_new = False

        cursor.execute(
            'insert into {} values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(args.game), keys + (hash,))

    connection.commit()
    connection.close()
