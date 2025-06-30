from collections import defaultdict
import csv
import pandas as pd
import requests
import time

import big_query.bq_utils as bqu
from big_query.bq_schemas import bq_schemas
import config as cf
from credentials.headers import headers
import utils


def register_countries():
    url = "https://api-football-v1.p.rapidapi.com/v3/countries"

    response = requests.get(url, headers=headers)
    countries = response.json()["response"]

    # dictionary containing the data
    countries_dict = defaultdict(list)

    for country in countries:
        # column names and values
        columns = [field.name for field in bq_schemas["countries"]]
        values = list(country.values())
        
        # adding row to dictionary
        for key, value in zip(columns, values):
            countries_dict[key].append(value) 
    
    # converting the dictionary to a DataFrame
    countries_df = pd.DataFrame(countries_dict)

    # uploading the data to the Google BQ table
    bqu.upload_data_to_bq(countries_df,
                          dataset_name="football_data",
                          table_name="countries",
                          mode="truncate")


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


def register_team(team_id: int, 
                  team_name: str,
                  team_logo: str) -> None:
    """
    Register team in the teams data table.

    Parameters:
    - team_id:      Unique identifier of the team.
    - team_name:    Name of the team.
    - team_logo:    URL to an image of the team logo.
    """
    # dictionary containing the team data
    team_dict = defaultdict(list)

    # column names and values
    columns = [field.name for field in bq_schemas["teams"]]
    values = [team_id, team_name, team_logo]
    
    # adding row to dictionary
    for key, value in zip(columns, values):
        team_dict[key].append(value) 

    # converting the team_dict to a DataFrame
    team_df = pd.DataFrame(team_dict)

    # uploading the data to the teams table on Google BQ
    bqu.upload_data_to_bq(team_df,
                          dataset_name="football_data",
                          table_name="teams",
                          mode="append")


def register_fixtures(date):
    """
    date (str): date as a string in the format "yyyy-mm-dd"
    """
    # make api call and obtain fixture data
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": date}
    response = requests.get(url, headers=headers, params=querystring)
    fixtures = response.json()["response"]

    # pre-registered teams
    teams_df = bqu.load_data_to_df(dataset_name="football_data",
                                   table_name="teams")
    team_ids = set(teams_df["team_id"])

    # dictionaries to store data
    fixtures_dict = defaultdict(list)
    
    # iterating through each fixture of the provided date
    for fixture in fixtures:
        # standard fixture data
        fixture_id, referee, timezone, date_str, _, _, venue_dict, status_dict = fixture["fixture"].values()

        date_list = date_str.split("T")
        date = date_list[0]
        kick_off = date_list[1].split("+")[0]

        venue_id, venue_name, city = venue_dict.values()
        _, _, elapsed_time, extra_time = status_dict.values()

        # league data
        league_id, league_name, _, _, _, season, stage, _ = fixture["league"].values()

        # teams data
        home_team_dict, away_team_dict = fixture["teams"].values()
        home_team_id, home_team_name, home_team_logo, _ = home_team_dict.values()
        away_team_id, away_team_name, away_team_logo, _ = away_team_dict.values()

        # registering home and away teams if they haven't been registered before
        if home_team_id not in team_ids:
            register_team(home_team_id, home_team_name, home_team_logo)

        if away_team_id not in team_ids:
            register_team(away_team_id, away_team_name, away_team_logo)

        # scores data
        halftime_dict, fulltime_dict, extratime_dict, penalty_dict = fixture["score"].values()

        home_ft, away_ft = fulltime_dict.values()
        home_ht, away_ht = halftime_dict.values()
        home_et, away_et = extratime_dict.values()
        home_pen, away_pen = penalty_dict.values()

        # column names and values
        columns = [field.name for field in bq_schemas["fixtures_all"]]
        values = [fixture_id, referee, timezone, date, kick_off, venue_id, venue_name, city,
                  elapsed_time, extra_time, league_id, league_name, season, stage,
                  home_team_id, home_team_name, away_team_id, away_team_name, home_ft,
                  away_ft, home_ht, away_ht, home_et, away_et, home_pen, away_pen]
        
        # adding row to dictionary
        for key, value in zip(columns, values):
            fixtures_dict[key].append(value) 

    # creating a dataframe of data to be uploaded
    fixtures_df = pd.DataFrame(fixtures_dict)

    # converting data type for date and kick_off
    fixtures_df["date"] = pd.to_datetime(fixtures_df["date"]).dt.date
    fixtures_df["kick_off"] = pd.to_datetime(fixtures_df["kick_off"], format="%H:%M:%S").dt.time

    # uploading fixture data to google big query
    bqu.upload_data_to_bq(df=fixtures_df,
                          dataset_name="football_data",
                          table_name="fixtures_all",
                          mode="append")


def register_manager(manager_id):
    """
    manager_id (int): id of the manager to be registered
    """
    if manager_id is not None:
        url = "https://api-football-v1.p.rapidapi.com/v3/coachs"

        querystring = {"id":f"{manager_id}"}
        
        response = requests.get(url, headers=headers, params=querystring)
        main_dict = response.json()["response"][0]

        name = main_dict["name"]
        firstname = main_dict["firstname"]
        lastname = main_dict["lastname"]

        birthdate = main_dict["birth"]["date"]
        birthplace = main_dict["birth"]["place"]
        birthcountry = main_dict["birth"]["country"]

        nationality = main_dict["nationality"]
        height = main_dict["height"]
        weight = main_dict["weight"]
        photo = main_dict["photo"]

        info = [manager_id,
                name,
                firstname,
                lastname,
                birthdate,
                birthplace,
                birthcountry,
                nationality,
                height,
                weight,
                photo
                ]

        with open("data/managers.csv", 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(info)


def register_player(player_id, season):
    """
    player_id (int): id of player to register
    season (int): season the player has featured in, should be equal to a year, e.g. 2024
    """  
    if player_id is not None:
        url = "https://api-football-v1.p.rapidapi.com/v3/players"
        querystring = {"id": f"{player_id}","season": f"{season}"}

        response = requests.get(url, headers=headers, params=querystring)
        main_dict = response.json()["response"][0]["player"]

        name = main_dict["name"]
        firstname = main_dict["firstname"]
        lastname = main_dict["lastname"]

        birthdate = main_dict["birth"]["date"]
        birthplace = main_dict["birth"]["place"]
        birthcountry = main_dict["birth"]["country"]

        nationality = main_dict["nationality"]
        height = main_dict["height"]
        weight = main_dict["weight"]
        photo = main_dict["photo"]

        info = [player_id,
                name,
                firstname,
                lastname,
                birthdate,
                birthplace,
                birthcountry,
                nationality,
                height,
                weight,
                photo
                ]

        with open("data/players.csv", 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(info)


def register_fixture(fixture_id):
    """
    fixture_id (int): id of the fixture to be registered 
    """
    registered_fixtures = utils.ids_from_csv("data/lineups_tactics.csv")
    if fixture_id in registered_fixtures:
        print("This fixture has already been registered!")
        return
    
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"id": f"{fixture_id}"}

    response = requests.get(url, headers=headers, params=querystring)
    main_dict = response.json()["response"][0]

    date_list = main_dict["fixture"]["date"].split("T")
    date = date_list[0]
    season = utils.season_from_fixture_id(fixture_id, date)

    any_new_managers = False
    any_new_players = False

    registered_managers = utils.ids_from_csv("data/managers.csv")
    registered_players = utils.ids_from_csv("data/players.csv")
    n_new_managers, n_new_players, new_managers, new_players = utils.find_new_managers_and_players(main_dict, 
                                                                                                   registered_managers, 
                                                                                                   registered_players
                                                                                                   )
    total_new_persons = n_new_managers + n_new_players

    if total_new_persons > 0:
        print(f"This fixture contains {n_new_managers} new managers and {n_new_players} new players ({total_new_persons} in total)")
        print("Do you wanna proceed [y/n]?")
        answer = input().strip().lower()

        if answer == "y":
            if total_new_persons > cf.REMAINING_PERSONS:
                any_new_players = True
                new_players_list = list(new_players)
                print(f"Registering players until {cf.REMAINING_PERSONS} persons are remaining in total")

                while total_new_persons > cf.REMAINING_PERSONS:
                    player_to_register = new_players_list[-1]
                    register_player(player_to_register, season)
                    new_players_list.pop()
                    total_new_persons -= 1
                
                print(f"{cf.REMAINING_PERSONS} persons are now remaining")
                print(f"Wait for {cf.DELAY_TIME} seconds to make more API calls")
                time.sleep(cf.DELAY_TIME)
                print("Wait time is over")
        
        elif answer == "n":
            return
        
        else:
            return "Invalid input! The input should be 'y' or 'n'"

    registered_managers = utils.ids_from_csv("data/managers.csv")
    registered_players = utils.ids_from_csv("data/players.csv")

    events = main_dict["events"]

    with open("data/events.csv", 'a', newline='') as file:
        writer_events = csv.writer(file)

        for event in events:
            time_elapsed = event["time"]["elapsed"]
            time_extra = event["time"]["extra"]
            team_id = event["team"]["id"]
            player_id = event["player"]["id"]
            assist_id = event["assist"]["id"]
            event_type = event["type"]
            detail = event["detail"]
            comment = event["comments"]

            info = [fixture_id, 
                    team_id, 
                    player_id,
                    time_elapsed, 
                    time_extra, 
                    event_type,
                    assist_id,
                    detail,
                    comment
                    ]
            
            writer_events.writerow(info)
    
    lineups = main_dict["lineups"]

    for lineup in lineups:
        team_id = lineup["team"]["id"]
        manager_id = lineup["coach"]["id"]
        formation = lineup["formation"]

        tactics_info = [fixture_id, 
                        team_id, 
                        manager_id, 
                        formation
                        ]
        
        if manager_id not in registered_managers:
            any_new_managers = True
            register_manager(manager_id)

        with open("data/lineups_tactics.csv", 'a', newline='') as file:
            writer_tactics = csv.writer(file)
            writer_tactics.writerow(tactics_info)

        with open("data/lineups.csv", 'a', newline='') as file:
            writer_lineups = csv.writer(file)

            start_xi = lineup["startXI"]

            for player in start_xi:
                player_dict = player["player"]
                player_id = player_dict["id"]
                number = player_dict["number"]
                grid = player_dict["grid"]
                
                player_info = [fixture_id, 
                               team_id, 
                               player_id, 
                               number, 
                               grid
                               ]

                if player_id not in registered_players:
                    any_new_players = True
                    register_player(player_id, season)

                writer_lineups.writerow(player_info)
        
        with open("data/substitutes.csv", 'a', newline='') as file:
            writer_subs = csv.writer(file)
            
            subs = lineup["substitutes"]
            subbed_ins = utils.get_subbed_in_players(fixture_id, team_id)

            for sub in subs:
                sub_dict = sub["player"]
                player_id = sub_dict["id"]
                number = sub_dict["number"]
                subbed_in = player_id in subbed_ins

                sub_info = [fixture_id,
                            team_id,
                            player_id,
                            number,
                            subbed_in
                            ]
                
                if player_id not in registered_players:
                    any_new_players = True
                    register_player(player_id, season)

                writer_subs.writerow(sub_info)
    
    # sort managers.csv and players.csv by id column
    if any_new_managers:
        utils.sort_csv_by_column(input_file="data/managers.csv", 
                                 output_file="data/managers.csv", 
                                 column_name="manager_id", 
                                 ascending=True
                                 )
        
    if any_new_players:
        utils.sort_csv_by_column(input_file="data/players.csv", 
                                 output_file="data/players.csv", 
                                 column_name="player_id", 
                                 ascending=True
                                 )

    with open("data/team_stats.csv", 'a', newline='') as file:
        writer_team_stats = csv.writer(file)

        team_stats = main_dict["statistics"]

        for team_stat in team_stats:
            team_id = team_stat["team"]["id"]

            stats_list = team_stat["statistics"]
            stats_info = [fixture_id, team_id] + [stat_dict["value"] for stat_dict in stats_list]
            stats_info = [0 if stat is None else stat for stat in stats_info]

            while len(stats_info) < 18:
                stats_info.append(None)
        
            writer_team_stats.writerow(stats_info)

    with open("data/player_stats.csv", 'a', newline='') as file:
        writer_player_stats = csv.writer(file)

        player_stats = main_dict["players"]

        for player_stat in player_stats:
            team_id = player_stat["team"]["id"]

            for player_stat_dict in player_stat["players"]:
                player_id = player_stat_dict["player"]["id"]
                stats_dict = player_stat_dict["statistics"][0]

                minutes = stats_dict["games"]["minutes"]
                rating = stats_dict["games"]["rating"]
                rating = float(rating) if rating is not None else 0.0
                captain = stats_dict["games"]["captain"]

                offsides = stats_dict["offsides"]
                
                total_shots = stats_dict["shots"]["total"]
                shots_on = stats_dict["shots"]["on"]

                goals_scored = stats_dict["goals"]["total"]
                goals_conceded = stats_dict["goals"]["conceded"]
                assists = stats_dict["goals"]["assists"]
                saves = stats_dict["goals"]["saves"]

                passes_attempted = stats_dict["passes"]["total"]
                passes_completed = stats_dict["passes"]["accuracy"]
                key_passes = stats_dict["passes"]["key"]

                tackles = stats_dict["tackles"]["total"]
                blocks = stats_dict["tackles"]["blocks"]
                interceptions = stats_dict["tackles"]["interceptions"]

                duels_total = stats_dict["duels"]["total"]
                duels_won = stats_dict["duels"]["won"]

                dribbles_attempted = stats_dict["dribbles"]["attempts"]
                dribbles_completed = stats_dict["dribbles"]["success"]
                dribbled_past = stats_dict["dribbles"]["past"]

                fouls_drawn = stats_dict["fouls"]["drawn"]
                fouls_committed = stats_dict["fouls"]["committed"]

                yellow_cards = stats_dict["cards"]["yellow"]
                red_cards = stats_dict["cards"]["red"]

                penalties_won = stats_dict["penalty"]["won"]
                penalties_committed = stats_dict["penalty"]["commited"]
                penalties_scored = stats_dict["penalty"]["scored"]
                penalties_missed = stats_dict["penalty"]["missed"]
                penalties_saved = stats_dict["penalty"]["saved"]

                player_stat_info = [fixture_id,
                                    team_id,
                                    player_id,
                                    minutes,
                                    rating,
                                    captain,
                                    offsides,
                                    total_shots,
                                    shots_on,
                                    goals_scored,
                                    goals_conceded,
                                    assists,
                                    saves,
                                    passes_attempted,
                                    passes_completed,
                                    key_passes,
                                    tackles,
                                    blocks,
                                    interceptions,
                                    duels_total,
                                    duels_won,
                                    dribbles_attempted,
                                    dribbles_completed,
                                    dribbled_past,
                                    fouls_drawn,
                                    fouls_committed,
                                    yellow_cards,
                                    red_cards,
                                    penalties_won,
                                    penalties_committed,
                                    penalties_scored,
                                    penalties_missed,
                                    penalties_saved]
                
                player_stat_info = [0 if stat is None else stat for stat in player_stat_info]

                writer_player_stats.writerow(player_stat_info)

    print(f"Fixture {fixture_id} has been registered successfully!")

    # commit changes to git
    files_to_commit = ["data/events.csv", 
                       "data/lineups_tactics.csv", 
                       "data/lineups.csv", 
                       "data/player_stats.csv", 
                       "data/substitutes.csv",
                       "data/team_stats.csv"
                       ]

    if any_new_managers:
        files_to_commit.append("data/managers.csv")
    if any_new_players:
        files_to_commit.append("data/players.csv")

    commit_message = f"registered fixture {fixture_id}"
    utils.commit_and_push_to_git(files_to_commit, commit_message)
