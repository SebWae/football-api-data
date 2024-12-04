import csv
import pandas as pd

def register_team(id, name, logo):
    with open('data/teams.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([id, name, logo])


def sort_csv_by_column(input_file, output_file, column_name, ascending=True):
    """
    Sorts a CSV file by a specific column and saves the result to a new file.

    Parameters:
        input_file (str): Path to the input CSV file.
        output_file (str): Path to save the sorted CSV file.
        column_name (str): Column name to sort by.
        ascending (bool): Whether to sort in ascending order (default True).

    Returns:
        None
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(input_file)
        
        # Check if the column exists in the DataFrame
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in the CSV file.")
        
        # Sort the DataFrame by the specified column
        sorted_df = df.sort_values(by=column_name, ascending=ascending)
        
        # Save the sorted DataFrame to the output file
        sorted_df.to_csv(output_file, index=False)
        print(f"CSV file sorted by column '{column_name}' and saved to '{output_file}'.")
    
    except Exception as e:
        print(f"Error: {e}")
