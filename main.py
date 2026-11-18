import json
import logging
import os
import signal
import time
import datetime as dt
from operator import index
import requests
from PSQLConnector.connector import PSQLConnection as Database
from dotenv import load_dotenv
from websockets.sync.client import connect

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("osu_worker")

expiration = 0
token = None

user_tasks: dict = {}

"""
osu api limit per minute
"""
api_limit = 60

"""
User cooldown in seconds
"""
user_cooldown = 1

"""
Score cooldown in seconds
"""
score_cooldown = 0.5

"""
How long in seconds that it will fetch new 1000 scores from osu api
"""
score_poll_cooldown = 5

uc_cost = api_limit / user_cooldown
sc_cost = api_limit / score_cooldown
spc_cost = api_limit / score_poll_cooldown

total_cost = (((uc_cost + sc_cost) / 2) + spc_cost) / 2
print(f"{uc_cost}")
print(sc_cost)
print(spc_cost)
print(f"Using {total_cost} out of {api_limit} ({round((total_cost/api_limit)*100, 2)}%)")

if total_cost > api_limit:
    logger.warning(f"Total cost {total_cost} is higher than api limit {api_limit}")

database = Database()

database.connect(
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    host=os.environ.get("DB_HOSTNAME"),
    database=os.environ.get("DB"),
)


def client_grant():
    global expiration
    global token
    logger.info("granting client token")

    response = requests.post(
        "https://osu.ppy.sh/oauth/token",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json={
            "client_id": os.environ.get("CLIENT_ID"),
            "client_secret": os.environ.get("OSU_SECRET"),
            "grant_type": "client_credentials",
            "scope": "public"
        }
    ).json()

    expiration = dt.datetime.now().timestamp() + response["expires_in"] - 60
    token = response["access_token"]

    return token


def check_access():
    global expiration
    global token
    # logger.info("checking access")

    try:
        if dt.datetime.now().timestamp() > expiration:
            logger.info("renewing token")
            token = client_grant()

    except Exception as E:
        logger.exception("error checking osu access token: %s", E)
        token = client_grant()

    return token


def get_users() -> list[int]:
    """
    Get all user ids in database that are currently in a match that hasn't ended
    :return: list of user ids
    """

    return [row[0] for row in database.fetch_all(
        "select distinct user_id from osu.match_users mu join osu.matches m on mu.match_id = m.id where m.ended = false"
    )]


def get_scores() -> list[int]:
    """
    Get all score ids in database
    :return: list of score ids
    """
    users = get_users()
    if len(users) == 0:
        return []

    return [row[0] for row in database.fetch_all(
        "select id from osu.scores where user_id in %s",
        params=(tuple(get_users()),)
    )]


def fetch_scores():
    """
    Fetches scores from osu api
    :return:
    """
    logger.info("fetching scores")

    score_req = requests.get(f"https://osu.ppy.sh/api/v2/scores/",
                             headers={"Authorization": f"Bearer {check_access()}"},
                             params={"mode": "osu"}).json()

    scores = score_req.get("scores")
    scores_check = get_scores()
    users_check = get_users()

    for score in scores:
        if (not score["id"] in scores_check) and score["user_id"] in users_check:
            if user_tasks.get(str(score["user_id"])) is not None:
                user_tasks[str(score["user_id"])].append(score)
            else:
                user_tasks[str(score["user_id"])] = [score]


def refresh_score(score_id: str):
    logger.info(f"Refreshing score {score_id}")
    requests.get(f"{os.environ.get('BACKEND', "http://gdcheerioscom:8080")}/api/osu/scores/push/{score_id}")
    return score_id


def refresh_user(user_id: str):
    logger.info(f"Refreshing user {user_id}")
    req = requests.get(f"{os.environ.get('BACKEND', "http://gdcheerioscom:8080")}/api/osu/fetch-user/{user_id}")
    return req.json()


ws = None
ws_retry_delay = 5


def close_websocket():
    global ws

    if ws is not None:
        try:
            ws.close()
        except Exception as error:
            logger.debug("Error closing WebSocket: %s", error)
        finally:
            ws = None


def send_websocket_event(event: dict):
    global ws
    message = json.dumps(event)

    while True:
        try:
            if ws is None:
                logger.info("Connecting to WebSocket server")
                ws = connect(os.environ.get("WS_URL", "ws://gdcheeriosws:8765"))

            ws.send(message)
            return
        except Exception as error:
            logger.warning("WebSocket connection/send failed: %s; retrying in %s seconds", error, ws_retry_delay)
            close_websocket()
            time.sleep(ws_retry_delay)


frame = 1
try:
    while True:
        logger.info(f"Frame {frame}")
        if len(user_tasks.keys()) > 0:
            logger.info("Completing refresh tasks")
            for user, scores in user_tasks.items():
                for score in scores:
                    id = refresh_score(score["id"])
                    send_websocket_event({"type": "new_score", "score_id": id, "user_id": user})
                    time.sleep(score_cooldown)
                    logger.info(f"Scores {scores.index(score) + 1}/{len(scores)} {round(((scores.index(score) + 1)/len(scores)) * 100, 2)}%")

                user_data = refresh_user(user)
                send_websocket_event({"type": "refresh", "user": user_data})
                time.sleep(user_cooldown)
                user_index = list(user_tasks.keys()).index(user)
                logger.info(f"Users {user_index + 1}/{len(user_tasks)} {round(((user_index + 1) / len(user_tasks)) * 100, 2)}%")

            user_tasks = {}
        else:
            fetch_scores()

        time.sleep(score_poll_cooldown)
        frame += 1
finally:
    close_websocket()
