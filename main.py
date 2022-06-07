import srcomapi
import srcomapi.datatypes as dt
import json
import pandas as pd
import argparse
import requests_cache
from tenacity import *
import os, errno

parser = argparse.ArgumentParser("Dump speedrun.com leaderboard to JSON/CSV")
parser.add_argument('game', metavar='G', nargs=None, help='id of game') 
parser.add_argument('category', metavar='C', nargs=None, help='name of category') 

args = parser.parse_args()

requests_cache.install_cache()

api = srcomapi.SpeedrunCom()
api.debug = 1

def get_game_leaderboards(game_id):
    game = api.get_game(game_id)

    lbs = {}
    for category in game.categories:
        if not category.name in lbs:
            lbs[category.name] = {}
        if category.type == 'per-level':
            for level in game.levels:
                lbs[category.name][level.name] = dt.Leaderboard(api, data=api.get(
                    "leaderboards/{}/level/{}/{}?embed=variables".format(game.id, level.id, category.id)))
        else:
            lbs[category.name] = dt.Leaderboard(api, data=api.get(
                "leaderboards/{}/category/{}?embed=variables".format(game.id, category.id)))

    return lbs

@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def append_run(r, runs):
    runs.append(
        {
            "players": list(map(lambda p: p.name, r.players)),
            "times": r.times['primary_t'],
            "platform": dt.Platform(api, api.get("platforms/{}".format(r.system['platform']))).name if not r.system['platform'] is None else "",
            "region": dt.Region(api, api.get("regions/{}".format(r.system['region']))).name if not r.system['region'] is None else "",
            "emulated": r.system['emulated'],
            "date": r.date,
            "comment": r.comment,
            "videos": list(map(lambda v: v['uri'], r.videos['links'])) if not r.videos is None else []
        }
    )

def json_encode_leaderboard(lb):
    runs = []
    
    for run in lb.runs:
        append_run(run['run'], runs)

    return json.dumps(runs)

lbs = get_game_leaderboards(args.game)

j = json_encode_leaderboard(lbs[args.category])

dir = 'out/{}{}'.format(args.game.strip(), args.category.strip())

try:
    os.makedirs("out")
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

with open(dir + '.json', 'w') as f:
    f.write(j)

with open(dir + '.csv', 'w') as f:
    f.write(pd.read_json(j).to_csv())