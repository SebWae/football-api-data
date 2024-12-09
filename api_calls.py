import requests
import csv
import pandas as pd
import utils
import time
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

    file_path_all = "fixtures_all.csv"
    file_path_selected = "fixtures_selected.csv"

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
    
    # sort teams.csv by id column
    utils.sort_csv_by_column(input_file="data/teams.csv", output_file="data/teams.csv", column_name="id", ascending=True)


def register_manager(manager_id):
    """
    manager_id (int): id of the manager to be registered
    """
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
            remaining_persons = 29
            if total_new_persons > remaining_persons:
                new_players_list = list(new_players)
                print(f"Registering players until {remaining_persons} persons are remaining in total")

                while total_new_persons > remaining_persons:
                    player_to_register = new_players_list[-1]
                    register_player(player_to_register, season)
                    new_players_list.pop()
                    total_new_persons -= 1
                    
                print(f"{remaining_persons} persons are now remaining")
                print("Wait for 1 minute to make more API calls")
                time.sleep(60)
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
                    register_player(player_id, season)

                writer_subs.writerow(sub_info)
    
    # sort managers.csv and players.csv by id column
    utils.sort_csv_by_column(input_file="data/managers.csv", 
                             output_file="data/managers.csv", 
                             column_name="manager_id", 
                             ascending=True
                             )
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
