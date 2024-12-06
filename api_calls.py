import requests
import csv
import pandas as pd
import os
import utils
from headers import headers

def get_countries():
    url = "https://api-football-v1.p.rapidapi.com/v3/countries"

    response = requests.get(url, headers=headers)
    countries = response.json()["response"]

    with open('data/countries.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "code", "flag"])

        for country in countries:
            name = country["name"]
            code = country["code"]
            flag = country["flag"]
            writer.writerow([name, code, flag])


def get_leagues(country):
    url = "https://api-football-v1.p.rapidapi.com/v3/leagues"

    querystring = {"country": country}

    response = requests.get(url, headers=headers, params=querystring)
    leagues = response.json()["response"]

    with open('data/leagues.csv', 'a', newline='') as file:
        writer = csv.writer(file)

        for league in leagues:
            id = league["league"]["id"]
            name = league["league"]["name"]
            type = league["league"]["type"]
            country = league["country"]["name"]
            first_season = league["seasons"][0]["year"]
            writer.writerow([id, name, type, country, first_season])


def register_fixtures(date):
    """
    date (str): date as a string in the format "yyyy-mm-dd"
    """
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": date}

    response = requests.get(url, headers=headers, params=querystring)
    fixtures = response.json()["response"]

    teams_df = pd.read_csv("data/teams.csv")
    team_ids = set(teams_df["id"])

    selected_leagues = {2, 3, 15, 39, 45, 48, 528, 531, 848}

    year = date[0:4]
    month = date[5:7]

    folder_path_all = f"data/fixtures_all/{year}/"
    folder_path_selected = f"data/fixtures_selected/{year}/"
    file_path_all = f"{folder_path_all}{year}_{month}_fixtures_all.csv"
    file_path_selected = f"{folder_path_selected}{year}_{month}_fixtures_selected.csv"

    # ensure directories exist
    os.makedirs(folder_path_all, exist_ok=True)
    os.makedirs(folder_path_selected, exist_ok=True)

    # headers for csv files
    headers_csv = [
        "id", "referee", "timezone", "date", "kick_off", "venue_id", "venue",
        "city", "elapsed_time", "extra_time", "league_id", "league", "season",
        "stage", "home_team_id", "home_team", "away_team_id", "away_team",
        "home_ft", "away_ft", "home_ht", "away_ht", "home_et", "away_et",
        "home_pen", "away_pen"
    ]

    # add headers to csv file for all fixtures if the file does not exist
    if not os.path.exists(file_path_all):
        with open(file_path_all, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers_csv)

    # add headers to csv file for selected fixtures if the file does not exist
    if not os.path.exists(file_path_selected):
        with open(file_path_selected, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers_csv)

    # add fixtures to csv files
    with open(file_path_all, 'a', newline='') as file:
        writer_all = csv.writer(file)

        for fixture in fixtures:
            id = fixture["fixture"]["id"]
            referee = fixture["fixture"]["referee"]
            timezone = fixture["fixture"]["timezone"]

            date_list = fixture["fixture"]["date"].split("T")
            date = date_list[0]
            kick_off = date_list[1].split("+")[0]

            venue_id = fixture["fixture"]["venue"]["id"]
            venue = fixture["fixture"]["venue"]["name"]
            city = fixture["fixture"]["venue"]["city"]

            elapsed_time = fixture["fixture"]["status"]["elapsed"]
            extra_time = fixture["fixture"]["status"]["extra"]

            league_id = fixture["league"]["id"]
            league = fixture["league"]["name"]
            season = fixture["league"]["season"]
            stage = fixture["league"]["round"]

            home_team_id = fixture["teams"]["home"]["id"]
            home_team = fixture["teams"]["home"]["name"]

            if home_team_id not in team_ids:
                home_team_logo = fixture["teams"]["home"]["logo"]
                utils.register_team(home_team_id, home_team, home_team_logo)

            away_team_id = fixture["teams"]["away"]["id"]
            away_team = fixture["teams"]["away"]["name"]

            if away_team_id not in team_ids:
                away_team_logo = fixture["teams"]["away"]["logo"]
                utils.register_team(away_team_id, away_team, away_team_logo)

            home_ft = fixture["score"]["fulltime"]["home"]
            away_ft = fixture["score"]["fulltime"]["away"]

            home_ht = fixture["score"]["halftime"]["home"]
            away_ht = fixture["score"]["halftime"]["away"]

            home_et = fixture["score"]["extratime"]["home"]
            away_et = fixture["score"]["extratime"]["away"]

            home_pen = fixture["score"]["penalty"]["home"]
            away_pen = fixture["score"]["penalty"]["away"]

            writer_all.writerow([
                id, referee, timezone, date, kick_off, venue_id, venue, city,
                elapsed_time, extra_time, league_id, league, season, stage,
                home_team_id, home_team, away_team_id, away_team, home_ft,
                away_ft, home_ht, away_ht, home_et, away_et, home_pen, away_pen
            ])
            
            # add fixture to csv file storing selected fixtures if league id is defined in selected_leagues set  
            if league_id in selected_leagues:
                with open(file_path_selected, 'a', newline='') as file:
                    writer_selected = csv.writer(file)

                    writer_selected.writerow([
                        id, referee, timezone, date, kick_off, venue_id, venue, city,
                        elapsed_time, extra_time, league_id, league, season, stage,
                        home_team_id, home_team, away_team_id, away_team, home_ft,
                        away_ft, home_ht, away_ht, home_et, away_et, home_pen, away_pen
                    ])

