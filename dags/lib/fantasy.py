"""
Fantasy Baseball

Retrieves, parses and inserts fantasy data into the postgres db
for later analysis.
"""

import json
import os
import re

import pandas as pd
import pybaseball
import requests
import sqlalchemy
from bs4 import BeautifulSoup

import db


ACTIVE_SEASON = 2023

# espn names are canon for this analysis - use those!
NAME_REPLACEMENTS = {}


def output_path(file_name: str) -> str:
    """
    Retrieves the global output folder and any files in it.
    """
    return os.path.join(os.environ.get("AIRFLOW_HOME"), "output", file_name)


def get_sqlalchemy_engine():
    """
    Create and return a SQLAlchemy engine for inserting into postgres.
    """
    # ## Write Information Back to Database
    #
    return sqlalchemy.create_engine(
        "postgresql://{user}:{password}@{host}:{port}/{db}".format(
            user=db.POSTGRES_USER,
            password=db.POSTGRES_PASSWORD,
            host=db.POSTGRES_IP,
            port=db.POSTGRES_PORT,
            db=db.POSTGRES_DB,
        ),
        isolation_level="AUTOCOMMIT",
    )


def pandas_parse_actuals(input_html, out_file_name):
    """
    Version of the above function that instead parses using Pandas.
    """
    all_dfs = pd.read_html(input_html)
    df = all_dfs[16]  # read_html returns all DFs, this is the one we need

    # columns are tuples for some reason
    df.columns = [replace_chars(x[1].lower()) for x in df.columns]

    # write dataframe out to CSV
    df.to_csv(output_path(out_file_name))
    df = df.head(-1)  # drop buggy last row

    # convert all applicable columns to numeric
    df = df.apply(pd.to_numeric, errors="ignore")

    # also write to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    table_name = os.path.splitext(out_file_name)[0]
    df.to_sql(table_name, conn, schema="fantasy", if_exists="replace")
    conn.execute(sqlalchemy.text(f"GRANT SELECT ON fantasy.{table_name} TO PUBLIC"))
    conn.close()


def get_fangraphs_actuals():
    """
    Return the actuals for each player.
    """
    # static urls
    PITCHERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=pit&lg=all&qual=0&type=c,36,37,38,40,-1,120,121,217,-1,24,41,42,43,44,-1,117,118,119,-1,6,45,124,-1,62,122,13&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_100000".format(
        season=ACTIVE_SEASON
    )
    BATTERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg=all&qual=0&type=8&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_10000".format(
        season=ACTIVE_SEASON
    )

    # # request the data
    pitchers_html = requests.get(PITCHERS_URL).text
    batters_html = requests.get(BATTERS_URL).text

    # Now that we have all of the player data, I'm writing these out to a CSV file if I want to load them again later without having to run the requests to those pages once more.
    pandas_parse_actuals(batters_html, "batters_actuals.csv")
    pandas_parse_actuals(pitchers_html, "pitchers_actuals.csv")


def parse_pctg(value):
    """
    Parse the text value for percentages out into a float.
    """
    return float(value.replace("%", "")) / 100


def get_all_fangraphs_pages():
    """
    Returns all of the 4 different Fangraphs Depth Charts projections
    from beginning of season and RoS.
    """
    get_fangraphs_data(
        "https://www.fangraphs.com/projections?pos=all&stats=bat&type=fangraphsdc",
        output_path("batters_projections_depth_charts.json"),
    )
    get_fangraphs_data(
        "https://www.fangraphs.com/projections?pos=all&stats=pit&type=fangraphsdc",
        output_path("pitchers_projections_depth_charts.json"),
    )


def get_fangraphs_data(url: str, out_file: str) -> None:
    """
    Retrieve the player data from a given fangraphs page and save the
    JSON to the file specified.
    """
    print(f"get_fangraphs_data {url} -> {out_file}")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, features="lxml")

    # values are stored as a next.js page
    # data is stored as the result of the first query. it does not seem to matter
    # what the page size is, all data is returned regardless.
    next_data = soup.find("script", {"id": "__NEXT_DATA__"})
    json_content = json.loads(next_data.text)
    data = json_content["props"]["pageProps"]["dehydratedState"]["queries"][0]["state"]["data"]

    with open(out_file, "w", newline="") as out_file:
        json.dump(data, out_file)


def post_fangraphs_projections_json_to_postgres(json_file: str) -> None:
    """
    Input one of the fangraphs html files and rip the first table we find in it.
    Writes the outputs to a csv in the output folder.
    """
    with open(json_file, "r+", encoding="utf8") as jfile:
        contents = json.load(jfile)
        df = pd.DataFrame.from_dict(contents)

    # clean up column names
    df.columns = [replace_chars(x.lower()) for x in df.columns]
    edit_columns(df)
    df["espnteamid"] = df["teamid"].apply(lambda x: to_espn_team_id(x))
    replace_names(df, "name")

    # create sqlalchemy engine for putting dataframe to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    table_name = os.path.splitext(os.path.basename(json_file))[0]
    df.to_sql(table_name, conn, schema="fantasy", if_exists="replace")
    conn.execute(sqlalchemy.text(f"GRANT SELECT ON fantasy.{table_name} TO PUBLIC"))
    conn.close()


def edit_columns(df: pd.DataFrame) -> None:
    """
    Apply any necessary edits to the columns.
    1. deletes
    2. renames
    """
    # keyed by the filename
    edits = {
        "renames": {
            "playername": "name",
        },
        "deletes": [
            # don't need the 'name' of the team if there's abbrev aka 'BOS'/'NYY' etc
            "shortname",
            # actually a link to the player page
            "name",
            # not populated with data
            "gb_pct",
            "gdp",
            "gdpruns",
            # not sure
            ".",
            # useless for pitchers/hitters
            "ibb",
            "bs",
        ],
    }
    df.drop(columns=[x for x in edits["deletes"] if x in df.columns], inplace=True)
    df.rename(columns=edits["renames"], inplace=True)


def post_all_fangraphs_projections_to_postgres():
    """
    Invoke parser for each of the projections retrieved from fangraphs.
    """
    post_fangraphs_projections_json_to_postgres(
        output_path("batters_projections_depth_charts.json")
    )
    post_fangraphs_projections_json_to_postgres(
        output_path("pitchers_projections_depth_charts.json")
    )


def get_statcast_batter_actuals():
    """
    Gets the relevant statcast metrics for batters.
    """
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    statcast_results = pybaseball.batting_stats_bref(season=ACTIVE_SEASON)
    replace_names(statcast_results, "Name")
    statcast_results.columns = [replace_chars(x.lower()) for x in statcast_results.columns]
    statcast_results.to_sql("batters_statcast_actuals", conn, schema="fantasy", if_exists="replace")
    conn.execute(sqlalchemy.text("grant select on fantasy.batters_statcast_actuals to public"))


def get_statcast_pitcher_actuals():
    """
    Gets the relevant statcast metrics for batters.
    """
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    statcast_results = pybaseball.pitching_stats_bref(season=ACTIVE_SEASON)
    replace_names(statcast_results, "Name")
    statcast_results.columns = [replace_chars(x.lower()) for x in statcast_results.columns]
    statcast_results.to_sql(
        "pitchers_statcast_actuals", conn, schema="fantasy", if_exists="replace"
    )
    conn.execute(sqlalchemy.text("grant select on fantasy.pitchers_statcast_actuals to public"))
    conn.close()


def get_statcast_batter_data():
    """
    This parses out the statcast information from the baseball savant website.
    """
    url = "https://baseballsavant.mlb.com/statcast_leaderboard?year={season}&player_type=resp_batter_id".format(
        season=ACTIVE_SEASON
    )
    response = requests.get(url)
    soup = BeautifulSoup(response.text, features="lxml")
    scripts = soup.find_all("script")
    data_script = scripts[3]  # hardcoding this for now, may need updates

    # decode the script. this loads the js and keys into the 'leaderboard_data' variable.
    json_text = extract_json_objects(data_script.string, "leaderboard_data = ")
    players_list = []
    for p in json_text:
        if isinstance(p, list):
            players_list = p
            break

    # write back the database
    df = pd.DataFrame(players_list)
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    df.to_sql("batters_statcast", conn, schema="fantasy", if_exists="replace")
    conn.execute(sqlalchemy.text("grant select on fantasy.batters_statcast to public"))
    conn.close()


def get_pitcherlist_top_100():
    """
    Retrieve the pitcher list top 100 by parsing the main page and picking the
    first article in the list.
    """
    url1 = "https://www.pitcherlist.com/category/articles/the-list/"
    user_agent_str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
    headers = {"User-Agent": user_agent_str}
    response = requests.get(url1, headers=headers)
    soup = BeautifulSoup(response.text, features="lxml")

    # id = leaderboard-table
    scripts = soup.find("a", {"class": "link"})
    url2 = scripts["href"]  # get the first link
    response = requests.get(url2, headers=headers)

    # extract data frames from HTML
    all_df = pd.read_html(response.text)
    the_list = all_df[-1]
    replace_names(the_list, "Pitcher")
    the_list = the_list.drop(["Badges", "Change"], axis=1)

    def find_tier(x: str, compiled_re: re.Pattern):
        matches = compiled_re.findall(x)
        if matches:
            return matches[0]
        return None

    tier_regex = re.compile("(T[\d]+)")
    the_list["Tier"] = the_list["Pitcher"].apply(lambda x: find_tier(x, tier_regex)).ffill()
    the_list["Pitcher"] = the_list["Pitcher"].apply(lambda x: tier_regex.sub("", x))
    the_list = the_list.apply(pd.to_numeric, errors="ignore")
    the_list = the_list.rename(columns={"Rank": "rank", "Pitcher": "name", "Tier": "tier"})

    # post to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    the_list.to_sql("pitchers_pitcherlist_100", conn, schema="fantasy", if_exists="replace")
    conn.execute(sqlalchemy.text("grant select on fantasy.pitchers_pitcherlist_100 to public"))


def extract_json_objects(text, start_str="{", decoder=json.JSONDecoder()):
    """Find JSON objects in text, and yield the decoded JSON data

    Does not attempt to look for JSON arrays, text, or other JSON types outside
    of a parent JSON object.

    Original stackoverflow post where I got this from:
    https://stackoverflow.com/questions/54235528/how-to-find-json-object-in-text-with-python
    """
    pos = 0
    while True:
        match = text.find(start_str, pos)
        if match == -1:
            break
        match += len(start_str)
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1


def replace_names(df, name_col):
    """
    Take the list of names that are known to misalign between different sources and
    align them to the name used in ESPN.
    """
    df[name_col] = df[name_col].apply(lambda x: NAME_REPLACEMENTS.get(x, x))


def replace_chars(input_str: str):
    """
    Given a string, perform a number of string replacements to avoid screwing up
    the underlying sql database.
    """
    replacements = {
        "#": "idx",
        "%": "_pct",
        "-": "_",
        "+": "_plus",
        "/": "_per_",
    }
    for k, v in replacements.items():
        input_str = input_str.replace(k, v)

    return input_str


def to_espn_team_id(fangraphs_team_id: int) -> int:
    """
    Translate the fangraphs team id to ESPN's one for easier joining.
    If no team, return 0 which is free agent in ESPN's system.
    """
    # fangraphs id -> espn id
    ids = {
        1: 3,  # LAA
        2: 1,  # BAL
        3: 2,  # BOS
        4: 4,  # CHW
        5: 5,  # CLE
        6: 6,  # DET
        7: 7,  # KCR
        8: 9,  # MIN
        9: 10,  # NYY
        10: 11,  # OAK
        11: 12,  # SEA
        12: 30,  # TBR
        13: 13,  # TEX
        14: 14,  # TOR
        15: 29,  # ARI
        16: 15,  # ATL
        17: 16,  # CHC
        18: 17,  # CIN
        19: 27,  # COL
        20: 28,  # MIA
        21: 18,  # HOU
        22: 19,  # LAD
        23: 8,  # MIL
        24: 20,  # WSN
        25: 21,  # NYM
        26: 22,  # PHI
        27: 23,  # PIT
        28: 24,  # STL
        29: 25,  # SDP
        30: 26,  # SFG
    }
    return ids.get(fangraphs_team_id, 0)


if __name__ == "__main__":
    print()
    # get_all_fangraphs_pages()
    post_all_fangraphs_projections_to_postgres()
    get_pitcherlist_top_100()
    # get_fangraphs_actuals()
    # get_statcast_batter_actuals()
    # get_statcast_pitcher_actuals()
    # get_statcast_batter_data()
