"""
ESPN data retrieval

Get data from the ESPN league and insert into Postgres.
"""

import requests
import csv
import datetime
import subprocess
import json
import os
import sqlalchemy
import psycopg2
from bs4 import BeautifulSoup

# connection information for the database
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_IP = "192.168.0.118"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"

# requests + espn auth data
ESPN_SWID = os.environ["ESPN_SWID"]
ESPN_S2 = os.environ["ESPN_S2"]

# ## Get Roster Data
# This will rip the roster information from ESPN and save it to a local CSV file.
ESPN_ROSTERS_URL = "http://fantasy.espn.com/apis/v3/games/flb/seasons/2019/segments/0/leagues/{league_id}?view=mDraftDetail&view=mPositionalRatings&view=mPendingTransactions&view=mLiveScoring&view=mSettings&view=mRoster&view=mTeam&view=modular&view=mNav"
ESPN_PLAYERS_URL = "http://fantasy.espn.com/apis/v3/games/flb/seasons/2019/segments/0/leagues/{league_id}?scoringPeriodId=0&view=kona_player_info"
ESPN_LEAGUE_ID = 15594


def output_path(file_name):
    """
    Retrieves the global output folder and any files in it.
    """
    return os.path.join(os.environ.get("AIRFLOW_HOME"), "output", file_name)


def get_postgres_connection():
    """
    Get and return a connection to the postgres database.
    """
    conn = psycopg2.connect(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_IP,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
    )
    return conn


def get_espn_headers():
    """
    Returns the correct set of headers for the ESPN request.
    """
    return {"X-Fantasy-Platform": "kona-PROD-955c44b415a96e5c22bf97778ec0ce85dc325233"}


def get_espn_cookies():
    """
    Returns the appropriate cookies for ESPN.
    """
    return {"swid": ESPN_SWID, "espn_s2": ESPN_S2}


def get_espn_league_data():
    """
    Looks up the league's roster data and returns it in JSON format.

    Follow-on parsing tasks:
    - league members.
    - league settings & information.
    - teams.
    - rosters for each team.
    - watchlists.
    - transaction counter
    - draft data.
    """
    league_data_raw = requests.get(
        ESPN_ROSTERS_URL.format(league_id=ESPN_LEAGUE_ID),
        cookies=get_espn_cookies(),
        headers=get_espn_headers(),
    )
    rosters_json = json.loads(league_data_raw.text)

    date_str = str(datetime.date.today())
    out_file_path = output_path("rosters" + date_str + ".json")
    with open(out_file_path, "w", newline="") as out_file:
        json.dump(rosters_json, out_file, indent=2)


def get_espn_player_data():
    """
    Use the ESPN player API in order to get information about the available players.
    """
    player_data = {
        "players": {
            "filterSlotIds": {"value": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 19]},
            "limit": 5000,
            "offset": 0,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "STANDARD"},
            "filterStatsForTopScoringPeriodIds": {
                "value": 1,
                "additionalValue": [
                    "002019",
                    "102019",
                    "002018",
                    "012019",
                    "022019",
                    "032019",
                    "042019",
                ],
            },
        }
    }
    headers = {"X-Fantasy-Filter": json.dumps(player_data)}
    t = requests.get(
        ESPN_PLAYERS_URL.format(league_id=ESPN_LEAGUE_ID),
        headers=headers,
        cookies=get_espn_cookies(),
    )
    players_json = json.loads(t.text)

    date_str = str(datetime.date.today())
    out_file_path = output_path("players" + date_str + ".json")
    with open(out_file_path, "w", newline="") as out_file:
        json.dump(players_json, out_file, indent=2)


def load_league_members_to_postgres():
    """
    Loads the list of league members from the json file to the
    postgres database.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS fantasy.members;
        CREATE TABLE IF NOT EXISTS fantasy.members (
            id serial primary key,
            displayName varchar(64),
            firstName varchar(32),
            espn_id varchar(64),
            isLeagueCreator boolean,
            isLeagueManager boolean,
            lastName varchar(32)
        );
        GRANT SELECT ON fantasy.members TO PUBLIC;
        """
    )

    # load the member data from the json output.
    date_str = str(datetime.date.today())
    with open(output_path("rosters" + date_str + ".json")) as json_file:
        roster_data = json.load(json_file)
        member_data = roster_data["members"]

    # loop through and insert each member into the table
    for mem in member_data:
        member_insert = tuple(
            [
                mem.get("displayName"),
                mem.get("firstName"),
                mem.get("id"),
                bool(mem.get("isLeagueCreator")),
                bool(mem.get("isLeagueManager")),
                mem.get("lastName"),
            ]
        )

        cur.execute("INSERT INTO fantasy.members VALUES (DEFAULT, %s, %s, %s, %s, %s, %s)", member_insert)

    # commit changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # get_espn_league_data()
    load_league_members_to_postgres()
