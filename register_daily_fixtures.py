import api_calls as ac
import utils
import requests
from headers import headers
import csv
import pandas as pd
import big_query.bq_utils as bqu



def register_fixtures(date):
    """
    date (str): date as a string in the format "yyyy-mm-dd"
    """
    # df = pd.read_csv("data/fixtures_all.csv")
    # registered_dates = set(df["date"])

    # if date in registered_dates:
    #     return f"Fixtures from {date} has already been registered!"

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": date}

    response = requests.get(url, headers=headers, params=querystring)
    fixtures = response.json()["response"]

    teams_df = bqu.load_data_to_df(dataset_name="football_data",
                         table_name="teams")
    team_ids = set(teams_df["id"])

    # selected_leagues = {2, 3, 15, 39, 45, 48, 61, 78, 135, 140, 528, 531, 848}

    # file_path_all = "data/fixtures_all.csv"
    # file_path_selected = "data/fixtures_selected.csv"

    new_selected_fixtures = False
    new_teams = False

    # add fixtures to csv files
    # with open(file_path_all, 'a', newline='') as file:
        # writer_all = csv.writer(file)

    fixture_data = {"fixture_id": [],
                    "referee": [],
                    "timezone": [],
                    "date": [],
                    "kick_off": [],
                    "venue_id": [],
                    "venue_name": [],
                    "city": [],
                    "elapsed_time": [],
                    "extra_time": [],
                    "league_id": [],
                    "league_name": [],
                    "season": [],
                    "stage": [],
                    "home_team_id": [],
                    "home_team_name": [],
                    "away_team_id": [],
                    "away_team_id": [],
                    "home_ft": [],
                    "away_ft": [],
                    "home_ht": [],
                    "away_ht": [],
                    "home_et": [],
                    "away_et": [],
                    "home_pen": [],
                    "away_pen": []
                    }

    for fixture in fixtures:
        fixture_id = fixture["fixture"]["id"]
        referee = fixture["fixture"]["referee"]
        timezone = fixture["fixture"]["timezone"]

        date_list = fixture["fixture"]["date"].split("T")
        date = date_list[0]
        kick_off = date_list[1].split("+")[0]

        venue_id = fixture["fixture"]["venue"]["id"]
        venue_name = fixture["fixture"]["venue"]["name"]
        city = fixture["fixture"]["venue"]["city"]

        elapsed_time = fixture["fixture"]["status"]["elapsed"]
        extra_time = fixture["fixture"]["status"]["extra"]

        league_id = fixture["league"]["id"]
        league_name = fixture["league"]["name"]
        season = fixture["league"]["season"]
        stage = fixture["league"]["round"]

        home_team_id = fixture["teams"]["home"]["id"]
        home_team_name = fixture["teams"]["home"]["name"]

        if home_team_id not in team_ids:
            # new_teams = True
            home_team_logo = fixture["teams"]["home"]["logo"]
            # utils.register_team(home_team_id, home_team, home_team_logo)

        away_team_id = fixture["teams"]["away"]["id"]
        away_team_name = fixture["teams"]["away"]["name"]

        if away_team_id not in team_ids:
            # new_teams = True
            away_team_logo = fixture["teams"]["away"]["logo"]
            # utils.register_team(away_team_id, away_team, away_team_logo)

        home_ft = fixture["score"]["fulltime"]["home"]
        away_ft = fixture["score"]["fulltime"]["away"]

        home_ht = fixture["score"]["halftime"]["home"]
        away_ht = fixture["score"]["halftime"]["away"]

        home_et = fixture["score"]["extratime"]["home"]
        away_et = fixture["score"]["extratime"]["away"]

        home_pen = fixture["score"]["penalty"]["home"]
        away_pen = fixture["score"]["penalty"]["away"]

            # writer_all.writerow([
            #     id, referee, timezone, date, kick_off, venue_id, venue, city,
            #     elapsed_time, extra_time, league_id, league, season, stage,
            #     home_team_id, home_team, away_team_id, away_team, home_ft,
            #     away_ft, home_ht, away_ht, home_et, away_et, home_pen, away_pen
            # ])
            
            # add fixture to csv file storing selected fixtures if league id is defined in selected_leagues set  
            # if league_id in selected_leagues:
            #     new_selected_fixtures = True
            #     with open(file_path_selected, 'a', newline='') as file:
            #         writer_selected = csv.writer(file)

            #         writer_selected.writerow([
            #             id, referee, timezone, date, kick_off, venue_id, venue, city,
            #             elapsed_time, extra_time, league_id, league, season, stage,
            #             home_team_id, home_team, away_team_id, away_team, home_ft,
            #             away_ft, home_ht, away_ht, home_et, away_et, home_pen, away_pen
            #         ])
    
    # commit changes to git
    # files_to_commit = ["data/fixtures_all.csv"]

    # # sorting teams.csv if new teams have been added
    # if new_teams:
    #     utils.sort_csv_by_column(input_file="data/teams.csv", output_file="data/teams.csv", column_name="id", ascending=True)
    #     files_to_commit.append("data/teams.csv")

    # if new_selected_fixtures:
    #     files_to_commit.append("data/fixtures_selected.csv")

    # commit_message = f"registered fixtures {date}"
    # utils.commit_and_push_to_git(files_to_commit, commit_message)

yesterday = utils.date_of_yesterday()
register_fixtures(yesterday)