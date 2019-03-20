"""
Fantasy Baseball

Retrieves, parses and inserts fantasy data into the postgres db
for later analysis.
"""

import requests
import csv
import datetime
import subprocess
import json
import os
import pandas as pd
import numpy as np
import sqlalchemy
import psycopg2
from bs4 import BeautifulSoup

# connection information for the database
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_IP = "192.168.0.118"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"


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
        "postgres://{user}:{password}@{host}:{port}/{db}".format(
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


def get_fangraphs_actuals():
    """
    Return the actuals for each player.
    """
    # static urls
    season = datetime.datetime.now().year
    PITCHERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=pit&lg=all&qual=0&type=c,36,37,38,40,-1,120,121,217,-1,24,41,42,43,44,-1,117,118,119,-1,6,45,124,-1,62,122,13&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_100000".format(
        season=season
    )
    BATTERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg=all&qual=0&type=8&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_10000".format(
        season=season
    )

    # # request the data
    pitchers_html = requests.get(PITCHERS_URL).text
    batters_html = requests.get(BATTERS_URL).text

    # Now that we have all of the player data, I'm writing these out to a CSV file if I want to load them again later without having to run the requests to those pages once more.
    parse_array_from_fangraphs_html(batters_html, "batters_actuals.csv")
    parse_array_from_fangraphs_html(pitchers_html, "pitchers_actuals.csv")


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
    with open(html_file, "rb") as bhtml:
        btxt = bhtml.read().decode("utf-8")
        # read_html returns ALL tables, we just want the last one.
        all_df = pd.read_html(btxt)
        df = all_df[-1]

    # get rid of na and clean column names
    df.dropna(axis=1, inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df.columns = [x.replace("%", "_pct").replace("/", "-").lower() for x in df.columns]
    for col in df.columns:
        if "_pct" in col:
            df[col] = df[col].apply(lambda x: parse_pctg(x))

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
    post_fangraphs_projections_html_to_postgres(output_path("batters_projections_depth_charts.html"))
    post_fangraphs_projections_html_to_postgres(output_path("batters_projections_depth_charts_ros.html"))
    post_fangraphs_projections_html_to_postgres(output_path("pitchers_projections_depth_charts.html"))
    post_fangraphs_projections_html_to_postgres(output_path("pitchers_projections_depth_charts_ros.html"))


def main():
    """
    Run the main loop in order to retrieve all of the data for both
    batting and pitching.
    """
    dfb_act = pd.read_csv(output_path("batters_actuals.csv"))
    dfp_act = pd.read_csv(output_path("pitchers_actuals.csv"))

    # apply that to all percentage values in the dataframes
    for col in dfb_act.columns:
        if "%" in col:
            dfb_act[col] = dfb_act[col].apply(lambda x: parse_pctg(x))

    for col in dfp_act.columns:
        if "%" in col:
            dfp_act[col] = dfp_act[col].apply(lambda x: parse_pctg(x))

    # rename columns to remove % (causes issues with postgres insert)
    dfb_act.columns = [
        x.replace("%", "_pct").replace("+", "_plus").replace("/", "-").lower()
        for x in dfb_act.columns
    ]
    dfp_act.columns = [
        x.replace("%", "_pct").replace("+", "_plus").replace("/", "-").lower()
        for x in dfp_act.columns
    ]

    # ## Filter and Qualify Information
    #
    # The dataframes for pitchers/batters contain a lot of noise for things that we don't really care about, or won't actually have much of an effect on our league.
    # apply some filters so we can get rid of players who won't play.
    # minimum plate appearances or innings pitched

    dfb = dfb[dfb["pa"] > 100]
    dfp = dfp[dfp["ip"] > 20]

    # ## Calculate Scores
    # The individual players in both the batting and pitching groups will get scored based on the entirety of the sample available. We calculate a composite score by taking the individual z-scores in each of the categories and trying to determine which players are above average.

    # a 1 represents a positive number, i.e. higher is better
    # a -1 represents negative, meaning lower is better
    dfb_score_cols = {"pa": 1, "k_pct": -1, "hr": 1, "iso": 1, "obp": 1, "woba": 1, "slg": 1}

    dfp_score_cols = {"ip": 1, "era": -1, "hr": -1, "so": 1, "whip": -1, "fip": -1, "k-9": 1}

    for col in dfb_score_cols.keys():
        col_score = col + "_score"
        dfb[col_score] = (
            (dfb[col] - dfb[col].mean()) / dfb[col].std(ddof=0) * dfb_score_cols.get(col)
        )

    for col in dfp_score_cols.keys():
        col_score = col + "_score"
        dfp[col_score] = (
            (dfp[col] - dfp[col].mean()) / dfp[col].std(ddof=0) * dfp_score_cols.get(col)
        )

    engine = get_sqlalchemy_engine()

    # open a connection and write the info back to the database
    conn = engine.connect()
    dfb.to_sql("batters", conn, schema="fantasy", if_exists="replace")
    dfb.to_sql("pitchers", conn, schema="fantasy", if_exists="replace")
    conn.close()


if __name__ == "__main__":
    get_all_fangraphs_pages()
    post_all_fangraphs_projections_to_postgres()
    # main()

