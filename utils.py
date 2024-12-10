import csv
import pandas as pd
import subprocess
from formations import formation_dict

def register_team(id, name, logo):
    with open('data/teams.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([id, name, logo])


def sort_csv_by_column(input_file, output_file, column_name, ascending=True, secondary_column=None):
    """
    Sorts a CSV file by a primary column, and optionally by a secondary column, and saves the result to a new file.

    Parameters:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the sorted CSV file.
        column_name (str): Primary column name to sort by.
        ascending (bool or list): Sort order for primary (and optionally secondary) column. Default is True.
        secondary_column (str, optional): Secondary column name for nested sorting. Default is None.

    Returns:
        None
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(input_file)
        
        # Check if the primary column exists in the DataFrame
        if column_name not in df.columns:
            raise ValueError(f"Primary column '{column_name}' not found in the CSV file.")
        
        # Check if the secondary column exists if provided
        if secondary_column and secondary_column not in df.columns:
            raise ValueError(f"Secondary column '{secondary_column}' not found in the CSV file.")
        
        # Store original dtypes to restore them after sorting
        original_dtypes = df.dtypes
        
        # Prepare columns and sort order for sorting
        sort_columns = [column_name]
        sort_order = [ascending] if isinstance(ascending, bool) else ascending

        if secondary_column:
            sort_columns.append(secondary_column)
            # If `ascending` is a single boolean, replicate it for secondary sort
            if isinstance(ascending, bool):
                sort_order.append(ascending)

        # Sort the DataFrame
        sorted_df = df.sort_values(by=sort_columns, ascending=sort_order)
        
        # Restore the original data types carefully
        for col in sorted_df.columns:
            if pd.api.types.is_integer_dtype(original_dtypes[col]) and sorted_df[col].isnull().any():
                # Fill NaN with a placeholder value before converting back to integer
                sorted_df[col] = sorted_df[col].fillna(0).astype(original_dtypes[col])
            else:
                # Directly convert to original dtype
                sorted_df[col] = sorted_df[col].astype(original_dtypes[col])
        
        # Save the sorted DataFrame to the output file
        sorted_df.to_csv(output_file, index=False)
        print(f"CSV file sorted by columns '{', '.join(sort_columns)}' and saved to '{output_file}'.")
    
    except Exception as e:
        print(f"Error: {e}")


def get_position(formation, grid):
    """
    formation (str): string indicating the position of the team, e.g., "4-4-2"
    grid (str): string indicating the chain and position of the player, e.g., "2:1"
    """
    
    position = formation_dict[formation][grid]

    return position


def ids_from_csv(csv_file_path):
    """
    csv_file_path (str): path to the csv file
    """
    df = pd.read_csv(csv_file_path)

    unique_ids = set(df.iloc[:, 0]) 

    return unique_ids


def season_from_fixture_id(fixture_id, fixture_date):
    """
    fixture_id (int): id of the fixture for which we want to find the season
    fixture_date (str): date of fixture on the format "yyyy-mm-dd"
    """
    df = pd.read_csv(f"data/fixtures_selected.csv")
    row = df[df['id'] == fixture_id]
    season = row['season'].iloc[0] if not row.empty else None

    return season


def get_subbed_in_players(fixture_id, team_id):
    """
    fixture_id (int): id of the relevant fixture
    team_id (int): id of the team to find subbed in players for
    """
    events_df = pd.read_csv("data/events.csv")

    relevant_events = events_df[(events_df['fixture_id'] == fixture_id) &
                                (events_df['team_id'] == team_id)
                                ]
    
    subs = relevant_events[relevant_events['event_type'] == 'subst']

    subbed_in_players = set(subs["assist_id"])
    
    return subbed_in_players


def find_new_managers_and_players(main_dict, existing_managers, existing_players):
    """
    main_dict (dict): dictionary item obtained from api call
    existing_managers (set): set of already registered managers
    existing_players (set): set of already registered players
    """
    manager_ids = set()
    player_ids = set()

    lineups = main_dict["lineups"]

    for lineup in lineups:
        manager_id = lineup["coach"]["id"]
        manager_ids.add(manager_id)

        for player in lineup["startXI"]:
            player_id = player["player"]["id"]
            player_ids.add(player_id)

        for player in lineup["substitutes"]:
            player_id = player["player"]["id"]
            player_ids.add(player_id)

    new_managers = manager_ids.difference(existing_managers)
    new_players = player_ids.difference(existing_players)

    n_new_managers = len(new_managers)
    n_new_players = len(new_players)

    return n_new_managers, n_new_players, new_managers, new_players


def commit_and_push_to_git(files_to_commit, commit_message):
    """
    files_to_commit (list): list of filenames to commit
    commit_message (str): commit message as a string
    """
    try:
        subprocess.run(["git", "add"] + files_to_commit, check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        print("Changes committed successfully!")
        subprocess.run(["git", "push"], check=True)
        print("Changes pushed successfully!")

    except subprocess.CalledProcessError as e:
        print(f"Error during Git operations: {e}")