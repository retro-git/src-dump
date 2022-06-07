import srcomapi
import srcomapi.datatypes as dt
import json

api = srcomapi.SpeedrunCom()
api.debug = 1

games = ["spyro1", "spyro2", "spyro3"]

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


lbs = get_game_leaderboards(games[1])

any = lbs["Any%"]

j = json_encode_leaderboard(any)

with open('data.json', 'w') as f:
    f.write(j)