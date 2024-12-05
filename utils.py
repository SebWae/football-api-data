import csv
import pandas as pd

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
        
        # Save the sorted DataFrame to the output file
        sorted_df.to_csv(output_file, index=False)
        print(f"CSV file sorted by columns '{', '.join(sort_columns)}' and saved to '{output_file}'.")
    
    except Exception as e:
        print(f"Error: {e}")
