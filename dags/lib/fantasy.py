"""
Fantasy Baseball

Retrieves, parses and inserts fantasy data into the postgres db
for later analysis.
"""

import csv
import datetime
import json
import os
import re
import subprocess

import numpy as np
import pandas as pd
import psycopg2
import pybaseball
import requests
import sqlalchemy
from bs4 import BeautifulSoup

# connection information for the database
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_IP = "192.168.0.118"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"

ACTIVE_SEASON = 2021

# espn names are canon for this analysis - use those!
NAME_REPLACEMENTS = {
    "Alexander Kirilloff": "Alex Kirilloff",
    "Alexander Lange": "Alex Lange",
    "Alexander Wells": "Alex Wells",
    "Brent Honeywell": "Brent Honeywell Jr.",
    "Caleb Raleigh": "Cal Raleigh",
    "Chad Tromp": "Chadwick Tromp",
    "Cedric Mullins II": "Cedric Mullins",
    "D.J. Johnson": "DJ Johnson",
    "D.J. Stewart": "DJ Stewart",
    "Dan Vogelbach": "Daniel Vogelbach",
    "Daniel Johnson Jr.": "Daniel Johnson",
    "Daniel Poncedeleon": "Daniel Ponce de Leon",
    "Dee Gordon": "Dee Strange-Gordon",
    "Donnie Walton": "Donovan Walton",
    "Eduardo Rodriguez": "Eduardo Rodríguez",
    "Giovanny Urshela": "Gio Urshela",
    "Gregory Deichmann": "Greg Deichmann",
    "Ha-Seong Kim": "Haseong Kim",
    "Ha-seong Kim": "Haseong Kim",
    "Hyun-Jin Ryu": "Hyun Jin Ryu",
    "Jacob Brentz": "Jake Brentz",
    "Jacob Lamb": "Jake Lamb",
    "Jason Groome": "Jay Groome",
    "Jasrado Chisholm": "Jazz Chisholm Jr.",
    "Javier Guerra": "Javy Guerra",
    "Joshua James": "Josh James",
    "Jonathan Brubaker": "JT Brubaker",
    "Kwang hyun Kim": "Kwang-Hyun Kim",
    "Kwang-hyun Kim": "Kwang-Hyun Kim",
    "Lance McCullers": "Lance McCullers Jr.",
    "Lourdes Gurriel": "Lourdes Gurriel Jr.",
    "Lucas Raley": "Luke Raley",
    "Luke Sims": "Lucas Sims",
    "Matt Boyd": "Matthew Boyd",
    "Matthew Cronin": "Matt Cronin",
    "Matthew Manning": "Matt Manning",
    "Michael Reed": "Mike Reed",
    "Michael Soroka": "Mike Soroka",
    "Michael Taylor": "Michael A. Taylor",
    "Nicholas Castellanos": "Nick Castellanos",
    "Nicholas Gordon": "Nick Gordon",
    "Nicholas Lodolo": "Nick Lodolo",
    "Peter Alonso": "Pete Alonso",
    "Samuel Clay": "Sam Clay",
    "Samuel Delaplane": "Sam Delaplane",
    "Samuel Long": "Sam Long",
    "Shed Long": "Shed Long Jr.",
    "Shohei Ohtani": "Shohei Ohtani",
    "Steven Souza": "Steven Souza Jr.",
    "Timothy Cate": "Tim Cate",
    "Travis D'Arnaud": "Travis d'Arnaud",
    "Vincent Velasquez": "Vince Velasquez",
    "Will Smith (RP)": "Will Smith",
    "William Vest": "Will Vest",
    "Yandy Diaz": "Yandy Díaz",
    "Yulieski Gurriel": "Yuli Gurriel",
    "Zacary Lowther": "Zac Lowther",
    "Zach Britton": "Zack Britton",
    "Zachary Short": "Zack Short",
    "Zachery Brown": "Zack Brown",
    "Zachery Pop": "Zach Pop",
}


def output_path(file_name):
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
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_IP,
            port=POSTGRES_PORT,
            db=POSTGRES_DB,
        )
    )


def parse_array_from_fangraphs_html(input_html, out_file_name):
    """
    Take a HTML stats page from fangraphs and parse it out to a CSV file.
    """
    # parse input
    soup = BeautifulSoup(input_html, "lxml")
    table = soup.find("table", {"class": "rgMasterTable"})

    # get headers
    headers_html = table.find("thead").find_all("th")
    headers = []
    for header in headers_html:
        headers.append(header.text)

    # get rows
    rows = []
    rows_html = table.find("tbody").find_all("tr")
    for row in rows_html:
        row_data = []
        cells = row.find_all("td")
        if len(cells) > 1:
            for cell in cells:
                row_data.append(cell.text)
            rows.append(row_data)

    # write to CSV file
    out_file_path = output_path(out_file_name)
    with open(out_file_path, "w") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(headers)
        writer.writerows(rows)


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
    df = df.head(-1) # drop buggy last row

    # convert all applicable columns to numeric
    df = df.apply(pd.to_numeric, errors='ignore')

    # also write to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    table_name = os.path.splitext(out_file_name)[0]
    df.to_sql(table_name, conn, schema="fantasy", if_exists="replace")
    conn.execute("GRANT SELECT ON fantasy.{} TO PUBLIC".format(table_name))
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
    return float(value.split()[0]) / 100


def get_all_fangraphs_pages():
    """
    Returns all of the 4 different Fangraphs Depth Charts projections
    from beginning of season and RoS.
    """
    subprocess.check_call("${AIRFLOW_HOME}/dags/lib/get_fangraphs.sh", shell=True)


def post_fangraphs_projections_html_to_postgres(html_file):
    """
    Input one of the fangraphs html files and rip the first table we find in it.
    Writes the outputs to a csv in the output folder.
    """
    with open(html_file, "r+", encoding="utf-8") as bhtml:

        # read the file and get rid of the pager table
        btxt = bhtml.read()
        soup = BeautifulSoup(btxt, "lxml")
        pager = soup.find_all("tr", attrs={"class": "rgPager"})
        if pager:
            for p in pager:
                p.decompose()  # remove pager
        validated_html = soup.prettify("utf-8")  # prettify for debug

        # read_html returns ALL tables, we just want the last one.
        all_df = pd.read_html(validated_html)
        df = all_df[-1]

    # get rid of na and clean column names
    df.columns = [x.lower() for x in df.columns]
    for column in df.columns:
        if "unnamed" in column:
            df.drop(column, axis=1, inplace=True)
    df.columns = [x.replace("%", "_pct").replace("/", "-").lower() for x in df.columns]
    for col in df.columns:
        if "_pct" in col:
            df[col] = df[col].apply(lambda x: parse_pctg(x))
    df.dropna(inplace=True)
    replace_names(df, "name")

    # create sqlalchemy engine for putting dataframe to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    table_name = os.path.splitext(os.path.basename(html_file))[0]
    df.to_sql(table_name, conn, schema="fantasy", if_exists="replace")
    conn.execute("GRANT SELECT ON fantasy.{} TO PUBLIC".format(table_name))
    conn.close()


def post_all_fangraphs_projections_to_postgres():
    """
    Invoke post_fangraphs_projections_html_to_postgres for each of the
    projections that we want.
    """
    post_fangraphs_projections_html_to_postgres(
        output_path("batters_projections_depth_charts.html")
    )
    post_fangraphs_projections_html_to_postgres(
        output_path("batters_projections_depth_charts_ros.html")
    )
    post_fangraphs_projections_html_to_postgres(
        output_path("pitchers_projections_depth_charts.html")
    )
    post_fangraphs_projections_html_to_postgres(
        output_path("pitchers_projections_depth_charts_ros.html")
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
    conn.execute("grant select on fantasy.batters_statcast_actuals to public")


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
    conn.execute("grant select on fantasy.pitchers_statcast_actuals to public")


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
    data_script = scripts[7]  # hardcoding this for now, may need updates

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
    conn.execute("grant select on fantasy.batters_statcast to public")


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
    the_list = all_df[1]
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
    the_list = the_list.apply(pd.to_numeric, errors='ignore')
    the_list = the_list.rename(columns={"Rank": "rank", "Pitcher": "name", "Tier": "tier"})

    # post to postgres
    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    the_list.to_sql("pitchers_pitcherlist_100", conn, schema="fantasy", if_exists="replace")
    conn.execute("grant select on fantasy.pitchers_pitcherlist_100 to public")


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
        "/": "_per",
    }
    for k, v in replacements.items():
        input_str = input_str.replace(k, v)
    
    return input_str


if __name__ == "__main__":
    get_all_fangraphs_pages()
    post_all_fangraphs_projections_to_postgres()
    get_fangraphs_actuals()
    get_statcast_batter_actuals()
    get_statcast_pitcher_actuals()
    get_statcast_batter_data()
    get_pitcherlist_top_100()
