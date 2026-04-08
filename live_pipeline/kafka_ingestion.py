from kafka import KafkaProducer
import time, json, requests

kafka_broker = "localhost:9092"
topic = "NBA-live-updates"
nba_scoreboard_url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"

producer = KafkaProducer(
    bootstrap_serveres = kafka_broker,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

def gather_live_updates():
    r = requests.get(nba_scoreboard_url, timeout=10)
    r.raise_for_status()
    return r.json["scoreboard"]["games"]

def run():
    while True:
        games = gather_live_updates()
        for game in games:
            event = {
                "game_id": game["gameId"],
                "h_team": game["homeTeam"]["teamTricode"],
                "a_team": game["awayTeam"]["teamTricode"],
                "h_points": game["homeTeam"]["score"],
                "a_points": game["awayTeam"]["score"],
                "quarter": game["period"],
                "clock": game["gameClock"],
                "game_status": game["gameStatusText"]
            }