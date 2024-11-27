import requests
import csv
import pandas as pd
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
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    querystring = {"date": date}

    response = requests.get(url, headers=headers, params=querystring)
    fixtures = response.json()["response"]

    teams_df = pd.read_csv("data/teams.csv")
    team_ids = set(teams_df["id"])

    with open('data/fixtures.csv', 'a', newline='') as file:
        writer = csv.writer(file)

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

            writer.writerow([id, 
                            referee, 
                            timezone,
                            date, 
                            kick_off,
                            venue_id, 
                            venue, 
                            city, 
                            elapsed_time, 
                            extra_time, 
                            league_id, 
                            league, 
                            season, 
                            stage, 
                            home_team_id, 
                            home_team, 
                            away_team_id, 
                            away_team,
                            home_ft,
                            away_ft,
                            home_ht,
                            away_ht,
                            home_et,
                            away_et,
                            home_pen,
                            away_pen
                            ])
    