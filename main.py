import srcomapi
import srcomapi.datatypes as dt
import json
import pandas as pd
import argparse

parser = argparse.ArgumentParser("Dump speedrun.com leaderboard to JSON/CSV")
parser.add_argument('game', metavar='G', nargs=None, help='id of game') 
parser.add_argument('category', metavar='C', nargs=None, help='name of category') 

args = parser.parse_args()

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

def json_encode_leaderboard(lb):
    runs = []
    
    for r in lb.runs:
        r = r['run']
        videos = []
        if not r.videos is None:
            videos = list(map(lambda v: v['uri'], r.videos['links']))

        runs.append(
            {
                "players": list(map(lambda p: p.name, r.players)),
                "times": r.times['primary_t'],
                "platform": api.get("platforms/{}".format(r.system['platform']))['name'],
                "region": api.get("regions/{}".format(r.system['region']))['name'],
                "emulated": r.system['emulated'],
                "date": r.date,
                "comment": r.comment,
                "videos": videos
            }
        )

    return json.dumps(runs)

lbs = get_game_leaderboards(args.game)

j = json_encode_leaderboard(lbs[args.category])

with open('data.json', 'w') as f:
    f.write(j)

with open('data.csv', 'w') as f:
    f.write(pd.read_json(j).to_csv())