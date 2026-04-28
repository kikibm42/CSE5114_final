from kafka import KafkaProducer
import time, json, requests

kafka_broker = "localhost:9092"
topic = "NBA-live-updates"
nba_scoreboard_url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
poll_interval = 5

producer = KafkaProducer(
    bootstrap_servers = kafka_broker,
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

# Pull data from endpoint
def gather_live_updates():
    r = requests.get(nba_scoreboard_url, timeout=10)
    r.raise_for_status()
    return r.json()["scoreboard"]["games"]

# Send game data to topic
def run():
    while True:
        try:
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
            
                producer.send(topic, value=event)
        
            producer.flush()
            
        except Exception as e:
            print(f"Error occured: {e}")

        time.sleep(poll_interval)

if __name__ == "__main__":
    run()
