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

@retry(wait=wait_exponential(multiplier=1, min=4, max=20))
def get_game_leaderboards(game_id):
    game = api.get_game(game_id)

    lbs = {}
    for category in game.categories:
        if not category.name in lbs:
            lbs[category.name] = {}
        if category.type == 'per-level' and args.il:
            for level in game.levels:
                lbs[category.name][level.name] = dt.Leaderboard(api, data=api.get(
                    "leaderboards/{}/level/{}/{}?embed=variables".format(game.id, level.id, category.id)))
        elif category.type == 'per-game' and not args.il:
            lbs[category.name] = dt.Leaderboard(api, data=api.get(
                "leaderboards/{}/category/{}?embed=variables".format(game.id, category.id)))

    return lbs

@retry(wait=wait_exponential(multiplier=1, min=4, max=20))
def append_run(r, runs):
    subcategory = ""
    if r.values:
        subcategory = ", ".join(
            list(map(lambda k: dt.Variable(api, api.get("variables/{}".format(k))).values["values"][r.values[k]]["label"], r.values.keys())))

    videos = ""
    if not r.videos is None:
        videos = ", ".join(list(map(lambda v: v['uri'], r.videos['links']))) if r.videos.get(
            "text") is None else r.videos.get("text")

    runs.append(
        {
            "subcategory": subcategory,
            "players": ", ".join(list(map(lambda p: p.name, r.players))),
            "times": r.times['primary_t'],
            "platform": dt.Platform(api, api.get("platforms/{}".format(r.system['platform']))).name if not r.system['platform'] is None else "",
            "region": dt.Region(api, api.get("regions/{}".format(r.system['region']))).name if not r.system['region'] is None else "",
            "emulated": r.system['emulated'],
            "date": r.date,
            "comment": r.comment,
            "videos": videos
        }
    )


def get_runs_list(lb):
    runs = []
    i = 0
    for run in lb.runs:
        print(i)
        append_run(run['run'], runs)
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

if args.sqlite:
    connection = sqlite3.connect('out/srcom.sqlite')
    cursor = connection.cursor()
    cursor.execute(
        'Create TABLE if not exists {} (category text, subcategory text, player text, time real, platform text, region text, emulated integer, date text, comment text, link text, [editor\'s note] text, cheated integer, removed integer, disputed integer, anonymised integer, unsubmitted integer, [no video] integer)'.format(args.game))

    columns = list(runs[0].keys())
    for row in runs:
        keys = (args.category,) + \
            tuple(row[c] for c in columns) + (None, False, False, False, False, False, True if row["videos"] == "" else False)
        hash = hashlib.sha256(repr(keys).encode()).hexdigest()
        cursor.execute(
            'insert into {} values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(args.game), keys)

    connection.commit()
    connection.close()
