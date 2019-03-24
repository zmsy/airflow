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
    with open(html_file, "r+", encoding="utf-8") as bhtml:

        # read the file and get rid of the pager table
        btxt = bhtml.read()
        soup = BeautifulSoup(btxt, "lxml")
        pager = soup.find('tr', attrs={'class':'rgPager'})
        if pager:
            pager.decompose() # remove pager
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
    # post_fangraphs_projections_html_to_postgres(output_path("batters_projections_depth_charts.html"))
    # post_fangraphs_projections_html_to_postgres(output_path("batters_projections_depth_charts_ros.html"))
    post_fangraphs_projections_html_to_postgres(output_path("pitchers_projections_depth_charts.html"))
    post_fangraphs_projections_html_to_postgres(output_path("pitchers_projections_depth_charts_ros.html"))


if __name__ == "__main__":
    # get_all_fangraphs_pages()
    post_all_fangraphs_projections_to_postgres()
    # main()

