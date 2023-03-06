"""
ESPN data retrieval

Get data from the ESPN league and insert into Postgres.
"""

import requests
import datetime
import json
import os
import psycopg2
from psycopg2.extras import execute_values

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
ESPN_ROSTERS_URL = "http://fantasy.espn.com/apis/v3/games/flb/seasons/{season}/segments/0/leagues/{league_id}?view=mDraftDetail&view=mPositionalRatings&view=mPendingTransactions&view=mLiveScoring&view=mSettings&view=mRoster&view=mTeam&view=modular&view=mNav"
ESPN_PLAYERS_URL = "http://fantasy.espn.com/apis/v3/games/flb/seasons/{season}/segments/0/leagues/{league_id}?scoringPeriodId=0&view=kona_player_info"
SEASON_ID = 2023
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
        ESPN_ROSTERS_URL.format(
            season=SEASON_ID,
            league_id=ESPN_LEAGUE_ID
        ),
        cookies=get_espn_cookies(),
        headers=get_espn_headers(),
    )
    rosters_json = json.loads(league_data_raw.text)

    date_str = str(datetime.date.today())
    out_file_path = output_path("rosters" + date_str + ".json")
    with open(out_file_path, "w", newline="") as out_file:
        json.dump(rosters_json, out_file)


def get_espn_player_data():
    """
    Use the ESPN player API in order to get information about the available players.
    """
    x_fantasy_filter = {
        "players": {
            "filterSlotIds": {"value": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 19]},
            "limit": 2500,
            "offset": 0,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            "sortDraftRanks": {"sortPriority": 100, "sortAsc": True, "value": "STANDARD"},
            "filterStatsForTopScoringPeriodIds": {
                "value": 1,
                "additionalValue": [],
            },
        }
    }

    headers = {"X-Fantasy-Filter": json.dumps(x_fantasy_filter)}
    data = requests.get(
        ESPN_PLAYERS_URL.format(
            season=SEASON_ID,
            league_id=ESPN_LEAGUE_ID
        ),
        headers=headers,
        cookies=get_espn_cookies(),
    )
    players_json = json.loads(data.text)

    date_str = str(datetime.date.today())
    out_file_path = output_path("players" + date_str + ".json")
    with open(out_file_path, "w", newline="") as out_file:
        json.dump(players_json, out_file)


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

        cur.execute(
            "INSERT INTO fantasy.members VALUES (DEFAULT, %s, %s, %s, %s, %s, %s)", member_insert
        )

    # commit changes and close the connection
    conn.commit()
    conn.close()


def load_teams_to_postgres():
    """
    Loads the list of teams in the rosters.json file to postgres.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS fantasy.teams;
        CREATE TABLE IF NOT EXISTS fantasy.teams (

            id serial primary key,
            abbrev varchar(10),
            divisionId integer,
            espn_id integer,
            logo varchar(255),

            logoType varchar(64),
            location varchar(64),
            nickname varchar(64),
            name varchar(128),
            playoffSeed integer,

            primaryOwner varchar(64),
            gamesBack numeric(6, 2),
            wins integer,
            losses integer,
            ties integer,

            acquisitions integer,
            drops integer,
            trades integer,
            moveToActive integer,
            moveToIR integer
        );
        GRANT SELECT ON fantasy.teams TO PUBLIC;
        """
    )

    # load the member data from the json output.
    date_str = str(datetime.date.today())
    with open(output_path("rosters" + date_str + ".json")) as json_file:
        roster_data = json.load(json_file)
        team_data = roster_data["teams"]

    # loop through and insert each member into the table
    for team in team_data:
        team_insert = tuple(
            [
                team.get("abbrev"),
                int(team.get("divisionId", 0)),
                team.get("id"),
                team.get("logo"),
                team.get("logoType"),
                team.get("location"),
                team.get("nickname"),
                " ".join([team.get("location"), team.get("nickname")]),
                int(team.get("playoffSeed", 0)),
                team.get("primaryOwner"),
                float(team.get("record", {}).get("overall", {}).get("gamesBack", 0.0)),
                int(team.get("record", {}).get("overall", {}).get("wins", 0.0)),
                int(team.get("record", {}).get("overall", {}).get("losses", 0.0)),
                int(team.get("record", {}).get("overall", {}).get("ties", 0.0)),
                int(team.get("transactionCounter", {}).get("acquisitions", 0)),
                int(team.get("transactionCounter", {}).get("drops", 0)),
                int(team.get("transactionCounter", {}).get("trades", 0)),
                int(team.get("transactionCounter", {}).get("moveToActive", 0)),
                int(team.get("transactionCounter", {}).get("moveToIR", 0)),
            ]
        )

        cur.execute(
            """
            INSERT INTO fantasy.teams VALUES (
                DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            team_insert,
        )

    # commit changes and close the connection
    conn.commit()
    conn.close()


def load_rosters_to_postgres():
    """
    Load the roster for each team to the database.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS fantasy.rosters;
        CREATE TABLE IF NOT EXISTS fantasy.rosters (

            id serial primary key,
            team_espn_id integer,
            acquisitionDate timestamp,
            acquisitionType varchar(24),
            injuryStatus varchar(24),

            playerId integer,
            defaultPositionId integer,
            active boolean,
            droppable boolean,
            firstName varchar(64),

            fullName varchar(128),
            lastName varchar(64)
        );
        GRANT SELECT ON fantasy.rosters TO PUBLIC;
        """
    )

    # load the member data from the json output.
    date_str = str(datetime.date.today())
    with open(output_path("rosters" + date_str + ".json")) as json_file:
        roster_data = json.load(json_file)
        team_data = roster_data["teams"]

    # loop through the teams and load rosters for each
    for team in team_data:
        for entry in team["roster"]["entries"]:
            roster_insert = tuple(
                [
                    team.get("id"),
                    datetime.datetime.fromtimestamp(entry["acquisitionDate"] / 1000),
                    entry.get("acquisitionType"),
                    entry.get("injuryStatus"),
                    entry.get("playerId"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("defaultPositionId"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("active"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("droppable"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("firstName"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("fullName"),
                    entry.get("playerPoolEntry", {}).get("player", {}).get("lastName"),
                ]
            )

            # execute the insert
            cur.execute(
                """
                INSERT INTO fantasy.rosters VALUES (
                    DEFAULT, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                """,
                roster_insert,
            )

    # commit changes and close the connection
    conn.commit()
    conn.close()


def load_players_to_postgres():
    """
    Loads the player entries to postgres.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS fantasy.players;
        CREATE TABLE IF NOT EXISTS fantasy.players (

            id serial primary key,
            espn_id integer,
            onTeamId integer,
            active boolean,
            defaultPositionId integer,

            auctionValue integer,
            draftRank integer,
            draftRankType varchar(24),
            droppable boolean,
            firstName varchar(64),

            fullName varchar(128),
            injured boolean,
            injuryStatus varchar(24),
            jersey varchar(12),
            lastName varchar(64),

            averageDraftPosition numeric(6, 2),
            auctionValueAverage numeric(6, 2),
            percentChange numeric(6, 2),
            percentOwned numeric(6, 2),
            percentStarted numeric(6, 2),
            proTeamId integer,
            rosterLocked boolean,

            status varchar(12),
            tradeLocked boolean,
            eligibility varchar(64),
            position varchar(64)
        );
        GRANT SELECT ON fantasy.players TO PUBLIC;
        """
    )

    # load the member data from the json output.
    date_str = str(datetime.date.today())
    with open(output_path("players" + date_str + ".json")) as json_file:
        players_data = json.load(json_file)
        players = players_data["players"]

    # loop through and insert each member into the table
    players_insert = []
    for player in players:
        player_insert = tuple(
            [
                player.get("id"),
                player.get("onTeamId"),
                player.get("player", {}).get("active", False),
                player.get("player", {}).get("defaultPositionId"),
                player.get("player", {})
                .get("draftRanksByRankType", {})
                .get("STANDARD", {})
                .get("auctionValue"),
                player.get("player", {})
                .get("draftRanksByRankType", {})
                .get("STANDARD", {})
                .get("rank"),
                player.get("player", {})
                .get("draftRanksByRankType", {})
                .get("STANDARD", {})
                .get("rankType"),
                player.get("player", {}).get("droppable", False),
                player.get("player", {}).get("firstName"),
                player.get("player", {}).get("fullName"),
                player.get("player", {}).get("injured", False),
                player.get("player", {}).get("injuryStatus"),
                player.get("player", {}).get("jersey"),
                player.get("player", {}).get("lastName"),
                player.get("player", {}).get("ownership", {}).get("averageDraftPosition"),
                player.get("player", {}).get("ownership", {}).get("auctionValueAverage"),
                player.get("player", {}).get("ownership", {}).get("percentChange"),
                player.get("player", {}).get("ownership", {}).get("percentOwned"),
                player.get("player", {}).get("ownership", {}).get("percentStarted"),
                player.get("player", {}).get("proTeamId"),
                player.get("rosterLocked"),
                player.get("status"),
                player.get("tradeLocked"),
                "|".join(get_player_eligibile_slots(player)),
                "|".join(get_player_position_eligibility(player)),
            ]
        )
        players_insert.append(player_insert)

    execute_values(
        cur,
        """
        INSERT INTO fantasy.players (
        espn_id, onTeamId, active, defaultPositionId,
        auctionValue, draftRank, draftRankType, droppable, firstName,
        fullName, injured, injuryStatus, jersey, lastName,
        averageDraftPosition, auctionValueAverage, percentChange,
        percentOwned, percentStarted, positionalRanking, totalRanking, totalRating,
        proTeamId, rosterLocked, status, tradeLocked, eligibility, position
        )
        VALUES %s
        """,
        players_insert,
    )

    # commit changes and close the connection
    conn.commit()
    conn.close()


def get_player_eligibile_slots(player):
    """
    Translates the "eligibleSlots" data from ESPN into readable player eligibility.
    Filters out the list for any positions that we don't support.
    """
    lineupSlots = dict(
        [
            (0, "C"),  # 1
            (1, "1B"),  # 1
            (2, "2B"),  # 1
            (3, "3B"),  # 1
            (4, "SS"),  # 1
            (5, "OF"),  # 5
            (6, "2B/SS"),  # 1
            (7, "1B/3B"),  # 1
            (12, "UTIL"),  # 1
            (13, "P"),  # 1
            (14, "SP"),  # 5
            (15, "RP"),  # 3
        ]
    )
    eligible_slots = player.get("player", {}).get("eligibleSlots")

    # pass all of the eligibility values to our lookup map
    eligibility_list = [lineupSlots.get(x) for x in eligible_slots]

    # filter any for positions that we don't have, or generic positions.
    eligibility_list = list(filter(lambda x: x is not None, eligibility_list))

    return eligibility_list


def get_player_position_eligibility(player):
    """
    From a list of eligible slots, return those that are actually positions and not
    just ESPN eligibility slots.
    """
    actual_positions = dict(
        [
            (0, "C"),  # 1
            (1, "1B"),  # 1
            (2, "2B"),  # 1
            (3, "3B"),  # 1
            (4, "SS"),  # 1
            (5, "OF"),  # 5
            (11, "DH"),  # 0
            (14, "SP"),  # 5
            (15, "RP"),  # 3
        ]
    )
    eligible_slots = player.get("player", {}).get("eligibleSlots")

    # pass all of the eligibility values to our lookup map
    eligibility_list = [actual_positions.get(x) for x in eligible_slots]

    # filter any for positions that we don't have, or generic positions.
    eligibility_list = list(filter(lambda x: x is not None, eligibility_list))

    return eligibility_list


def insert_etl_timestamp():
    """
    Send the timestamp to the database for the last time that the stats were updated.
    """
    conn = get_postgres_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SET TIMEZONE='America/Los_angeles';
        CREATE TABLE IF NOT EXISTS fantasy.etl_updates (
            id serial primary key,
            updated_time timestamptz
        );
        GRANT SELECT ON fantasy.etl_updates TO PUBLIC;
        INSERT INTO fantasy.etl_updates VALUES
        (DEFAULT, NOW());
        """
    )

    # commit changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    get_espn_league_data()
    get_espn_player_data()
    load_players_to_postgres()
    load_league_members_to_postgres()
    load_teams_to_postgres()
    load_rosters_to_postgres()
    insert_etl_timestamp()
