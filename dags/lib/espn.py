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
from typing import Dict

import db
from const import ACTIVE_SEASON
from util import output_path

# requests + espn auth data
ESPN_SWID = os.environ["ESPN_SWID"]
ESPN_S2 = os.environ["ESPN_S2"]

# ## Get Roster Data
# This will rip the roster information from ESPN and save it to a local CSV file.
ESPN_ROSTERS_URL = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{season}/segments/0/leagues/{league_id}?view=mSettings&view=mRoster&view=mTeam&view=modular&view=mNav"
ESPN_PLAYERS_URL = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{season}/players?scoringPeriodId=0&view=kona_player_info"
ESPN_LEAGUE_ID = 15594


def get_postgres_connection():
    """
    Get and return a connection to the postgres database.
    """
    conn = psycopg2.connect(
        user=db.POSTGRES_USER,
        password=db.POSTGRES_PASSWORD,
        host=db.POSTGRES_IP,
        port=db.POSTGRES_PORT,
        database=db.POSTGRES_DB,
    )
    return conn


def get_espn_headers() -> Dict[str, str]:
    """
    Returns the correct set of headers for the ESPN request.
    """
    return {"X-Fantasy-Platform": "kona-PROD-e831827300039bb6b4959fb881cf960295cc32d8"}


def get_espn_cookies() -> Dict[str, str]:
    """
    Returns the appropriate cookies for ESPN.
    """
    # return {"swid": ESPN_SWID, "espn_s2": ESPN_S2}
    return {}


def get_espn_player_data():
    x_fantasy_filter = {  # type: ignore
        "players": {
            "filterSlotIds": {
                "value": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 19]
            },
            "filterRanksForScoringPeriodIds": {"value": [6]},
            "limit": 1500,
            "offset": 0,
            "sortPercOwned": {"sortAsc": False, "sortPriority": 1},
            "sortDraftRanks": {
                "sortPriority": 100,
                "sortAsc": True,
                "value": "STANDARD",
            },
            "filterRanksForRankTypes": {"value": ["STANDARD"]},
            "filterStatsForTopScoringPeriodIds": {
                "value": 5,
                "additionalValue": [
                    "002025",
                    "102025",
                    "002024",
                    "012025",
                    "022025",
                    "032025",
                    "042025",
                    "062025",
                    "010002025",
                ],
            },
        }
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0",
        "Accept": "application/json",
        "Referer": "https://fantasy.espn.com/",
        "Origin": "https://fantasy.espn.com",
        "X-Fantasy-Filter": json.dumps(x_fantasy_filter),
        "X-Fantasy-Source": "kona",
        "X-Fantasy-Platform": "kona-PROD-e831827300039bb6b4959fb881cf960295cc32d8",
    }

    response = requests.get(
        ESPN_PLAYERS_URL.format(season=ACTIVE_SEASON),
        headers=headers,
    )

    response.raise_for_status()
    players_json = response.json()

    date_str = datetime.date.today().isoformat()
    out_file_path = f"players_{date_str}.json"

    with open(out_file_path, "w", newline="") as out_file:
        json.dump(players_json, out_file)

    return players_json


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
            "INSERT INTO fantasy.members VALUES (DEFAULT, %s, %s, %s, %s, %s, %s)",
            member_insert,
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
                team.get("name"),
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
                    entry.get("playerPoolEntry", {})
                    .get("player", {})
                    .get("defaultPositionId"),
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
            averageDraftPositionPercentChange numeric(6, 2),
            auctionValueAverage numeric(6, 2),
            auctionValueAverageChange numeric(6, 2),
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
        player_insert = tuple(  # type: ignore
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
                player.get("player", {})
                .get("ownership", {})
                .get("averageDraftPosition"),
                player.get("player", {})
                .get("ownership", {})
                .get("averageDraftPositionPercentChange"),
                player.get("player", {})
                .get("ownership", {})
                .get("auctionValueAverage"),
                player.get("player", {})
                .get("ownership", {})
                .get("auctionValueAverageChange"),
                player.get("player", {}).get("ownership", {}).get("percentChange"),
                player.get("player", {}).get("ownership", {}).get("percentOwned"),
                player.get("player", {}).get("ownership", {}).get("percentStarted"),
                player.get("player", {}).get("proTeamId"),
                player.get("rosterLocked"),
                player.get("status"),
                player.get("tradeLocked"),
                "|".join(get_player_eligibile_slots(player)),  # type: ignore
                "|".join(get_player_position_eligibility(player)),
            ]
        )
        players_insert.append(player_insert)

    execute_values(
        cur,
        """
        INSERT INTO fantasy.players (
            espn_id,
            onTeamId,
            active,
            defaultPositionId,
            auctionValue,
            draftRank,
            draftRankType,
            droppable,
            firstName,
            fullName,
            injured,
            injuryStatus,
            jersey,
            lastName,
            averageDraftPosition,
            averageDraftPositionPercentChange,
            auctionValueAverage,
            auctionValueAverageChange,
            percentChange,
            percentOwned,
            percentStarted,
            proTeamId,
            rosterLocked,
            status,
            tradeLocked,
            eligibility,
            position
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


if __name__ == "__main__":
    # get_espn_league_data()
    # get_espn_player_data()
    load_players_to_postgres()
    # load_league_members_to_postgres()
    # load_teams_to_postgres()
    # load_rosters_to_postgres()
