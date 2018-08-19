"""
Fantasy Baseball

Retrieves, parses and inserts fantasy data into the postgres db
for later analysis.
"""

import requests
import csv
import datetime
import subprocess
import os
import pandas as pd
import numpy as np
import sqlalchemy
import psycopg2
from bs4 import BeautifulSoup
from utilities import output_path
pd.options.display.max_columns = 150

# connection information for the database
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_IP = "192.168.0.118"
POSTGRES_PORT = 5432
POSTGRES_DB = 'postgres'


# ## Get Roster Data
# This will rip the roster information from ESPN and save it to a local CSV file.
LEAGUE_URL = "http://games.espn.com/flb/leaguerosters?leagueId={league_id}"
LEAGUE_ID = 15594


def get_rosters():
    """
    Looks up the league's roster data and returns it in CSV format.
    """
    rosters_html = requests.get(LEAGUE_URL.format(league_id=LEAGUE_ID)).text
    rosters_soup = BeautifulSoup(rosters_html, "lxml")
    rosters = rosters_soup.find_all("table", {'class': 'playerTableTable'})

    players = []
    for roster in rosters:
        team_name = roster.find("a").text
        players_html = roster.find_all("td", {'class': 'playertablePlayerName'})
        for player in players_html:
            # parse player info
            player_name = player.text.split(",")[0]
            player_name = player_name.replace("*", "")

            TRANSLATIONS = {
                'Nicky Delmonico': 'Nick Delmonico',
                'Yuli Gurriel': 'Yulieski Gurriel'
            }

            #translate player name if necessary
            translation = TRANSLATIONS.get(player_name)
            if translation:
                player_name = translation

            # add to output list
            players.append([player_name, team_name])

    with open(output_path("rosters.csv"), "w", newline='') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(("Name", "Squad"))
        writer.writerows(players)


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
        for cell in row.find_all("td"):
            row_data.append(cell.text)
        rows.append(row_data)
    
    # write to CSV file
    with open(output_path(out_file_name), "w") as out_file:
        writer = csv.writer(out_file)
        writer.writerow(headers)
        writer.writerows(rows)


def parse_pctg(value):
    """
    Parse the text value for percentages out into a float.
    """
    return float(value.split()[0]) / 100


def league(team_name):
    """
    Since FG data didnt come with league info, create a dict of league
    info for lookups.
    """

    LEAGUES = {
        'Angels': 'AL',
        'Astros': 'AL',
        'Athletics': 'AL',
        'Blue Jays': 'AL',
        'Braves': 'NL',
        'Brewers': 'NL',
        'Cardinals': 'NL',
        'Cubs': 'NL',
        'Diamondbacks': 'NL',
        'Dodgers': 'NL',
        'Giants': 'NL',
        'Indians': 'AL',
        'Mariners': 'AL',
        'Marlins': 'NL',
        'Mets': 'NL',
        'Nationals': 'NL',
        'Orioles': 'AL',
        'Padres': 'NL',
        'Phillies': 'NL',
        'Pirates': 'NL',
        'Rangers': 'AL',
        'Rays': 'AL',
        'Red Sox': 'AL',
        'Reds': 'NL',
        'Rockies': 'NL',
        'Royals': 'AL',
        'Tigers': 'AL',
        'Twins': 'AL',
        'White Sox': 'AL',
        'Yankees': 'AL'
    }

    return LEAGUES.get(team_name)


def main():
    """
    Run the main loop in order to retrieve all of the data for both
    batting and pitching.
    """

    # get the rosters and write them to disk
    get_rosters()

    # ## Get and Parse Actuals
    # 
    # Looks through the URLs to grab batting & pitching actuals and deliver those back to the user.

    # static urls
    season = datetime.datetime.now().year
    PITCHERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=pit&lg=all&qual=0&type=c,36,37,38,40,-1,120,121,217,-1,24,41,42,43,44,-1,117,118,119,-1,6,45,124,-1,62,122,13&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_100000".format(season=season)
    BATTERS_URL = "https://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg=all&qual=0&type=8&season={season}&month=0&season1={season}&ind=0&team=0&rost=0&age=0&filter=&players=0&page=1_10000".format(season=season)

    # # request the data
    pitchers_html = requests.get(PITCHERS_URL).text
    batters_html = requests.get(BATTERS_URL).text


    # Now that we have all of the player data, I'm writing these out to a CSV file if I want to load them again later without having to run the requests to those pages once more.
    parse_array_from_fangraphs_html(batters_html, 'batters_actuals.csv')
    parse_array_from_fangraphs_html(pitchers_html, 'pitchers_actuals.csv')


    # ## Get Projections
    # 
    # For this part, we need to call some external bash code here, because the form data is too big to reasonably bring into the script here. Check out the [original blog post](https://zmsy.co/blog/fantasy-baseball/) on how to configure this for your own purposes.
    subprocess.check_call('./get_fangraphs.sh', shell=True)

    # ## Read Data Into Pandas
    # 
    # Load those CSV files using read_csv() in pandas. Since some of the percentage values are stored as strings, we need to parse those into floats.
    # We want to create two dataframes here:
    # 
    # - `dfb` - Batters Dataframe
    # - `dfp` - Pitchers Dataframe
    df_rost = pd.read_csv(output_path('rosters.csv'))
    dfb_act = pd.read_csv(output_path('batters_actuals.csv'))
    dfp_act = pd.read_csv(output_path('pitchers_actuals.csv'))

    # apply that to all percentage values in the dataframes
    for col in dfb_act.columns:
        if '%' in col:
            dfb_act[col] = dfb_act[col].apply(lambda x: parse_pctg(x))
            
    for col in dfp_act.columns:
        if '%' in col:
            dfp_act[col] = dfp_act[col].apply(lambda x: parse_pctg(x))

    # rename columns to remove % (causes issues with postgres insert)
    df_rost.columns = [x.lower() for x in df_rost.columns]
    dfb_act.columns = [x.replace("%", "_pct").replace('+', '_plus').replace("/", "-").lower() for x in dfb_act.columns]
    dfp_act.columns = [x.replace("%", "_pct").replace('+', '_plus').replace("/", "-").lower() for x in dfp_act.columns]


    with open(output_path('batters_projections.html'), 'r') as bhtml:
        btxt = bhtml.read()
        dfb_proj = pd.read_html(btxt)[-1]  # read_html returns ALL tables, we just want the last one.
        dfb_proj.dropna(axis=1, inplace=True)

    with open(output_path('pitchers_projections.html'), 'r') as phtml:
        ptxt = phtml.read()
        dfp_proj = pd.read_html(ptxt)[-1]
        dfp_proj.dropna(axis=1, inplace=True)

    # rename columns and apply naming scheme
    dfb_proj.columns = [x.replace("%", "_pct").replace('/', '-').lower() for x in dfb_proj.columns]
    dfp_proj.columns = [x.replace("%", "_pct").replace('/', '-').lower() for x in dfp_proj.columns]

    # join the datasets together. we want one
    # for batters and one for pitchers, with
    # roster information in both of them.
    dfb = pd.merge(dfb_proj, df_rost, how='left', on='name', suffixes=('.p', '.r'))
    dfb = pd.merge(dfb, dfb_act, how='left', on='name', suffixes=('', '.a'))

    dfp = pd.merge(dfp_proj, df_rost, how='left', on='name', suffixes=('.p', '.r'))
    dfp = pd.merge(dfp, dfp_act, how='left', on='name', suffixes=('', '.a'))


    # ## Filter and Qualify Information
    # 
    # The dataframes for pitchers/batters contain a lot of noise for things that we don't really care about, or won't actually have much of an effect on our league.
    # apply some filters so we can get rid of players who won't play.
    # minimum plate appearances or innings pitched

    dfb = dfb[dfb['pa'] > 100]
    dfp = dfp[dfp['ip'] > 20]
    dfb['league'] = dfb['team'].apply(lambda x: league(x))
    dfp['league'] = dfp['team'].apply(lambda x: league(x))

    # rearrange columns for readability and filter some out
    # keep only ones relevant for our league
    dfb_columns = ['#', 'name', 'squad', 'team', 'league', 'pa', 'pa.a', 'ab', 'h', 'so', 'k_pct', 'hr', 'hr.a', 'avg', 'iso', 'babip', 'wrc_plus', 'avg.a', 'obp', 'obp.a', 'woba', 'woba.a', 'slg', 'slg.a', 'ops', 'bb_pct', 'bb']

    dfp_columns = ['#', 'name', 'squad', 'team', 'league', 'ip', 'era', 'er', 'hr', 'so', 'bb', 'whip', 'k-9', 'bb-9', 'fip', 'k-9.a', 'bb-9.a', 'k-bb', 'k_pct', 'whip.a', 'so.a', 'lob_pct', 'era.a', 'fip.a', 'e-f', 'xfip', 'siera', 'ip.a']

    # filter to just the columns we want
    dfb = dfb[dfb_columns]
    dfp = dfp[dfp_columns]


    # ## Calculate Scores
    # The individual players in both the batting and pitching groups will get scored based on the entirety of the sample available. We calculate a composite score by taking the individual z-scores in each of the categories and trying to determine which players are above average.

    # a 1 represents a positive number, i.e. higher is better
    # a -1 represents negative, meaning lower is better
    dfb_score_cols = {
        'pa': 1,
        'k_pct': -1,
        'hr': 1,
        'iso': 1,
        'obp': 1,
        'woba': 1,
        'slg': 1
    }

    dfp_score_cols = {
        'ip': 1,
        'era': -1,
        'hr': -1,
        'so': 1,
        'whip': -1,
        'fip': -1,
        'k-9': 1
    }

    for col in dfb_score_cols.keys():
        col_score = col + "_score"
        dfb[col_score] = (dfb[col] - dfb[col].mean()) / dfb[col].std(ddof=0) * dfb_score_cols.get(col)
        

    for col in dfp_score_cols.keys():
        col_score = col + "_score"
        dfp[col_score] = (dfp[col] - dfp[col].mean()) / dfp[col].std(ddof=0) * dfp_score_cols.get(col)


    # ## Write Information Back to Database
    # 
    # Once the table is available in the database, then we can query it again using other tools to make our lives easier. Then it can be used to display the information about each player in the Superset DB.
    engine = sqlalchemy.create_engine("postgres://{user}:{password}@{host}:{port}/{db}".format(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_IP,
        port=POSTGRES_PORT,
        db=POSTGRES_DB
    ))
    conn = engine.connect()

    dfb.to_sql('batters', conn, schema='fantasy', if_exists='replace')
    dfb.to_sql('pitchers', conn, schema='fantasy', if_exists='replace')

    conn.close()



if __name__ == '__main__':
    main()

