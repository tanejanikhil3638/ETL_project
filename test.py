import pandas as pd
import os

# Step 1: Extract - Load data from CSV files (source and target)
def extract_data(source_file, target_file):
    source_data = pd.read_csv(source_file)
    target_data = pd.read_csv(target_file)
    return source_data, target_data

# Step 2: Transform - Add constraints to match source data with target data
def transform_data(source_data, target_data, new_columns, append_after, append_before, dropdown_options):
    # Add constraints here (for example, renaming columns, reordering columns, etc.)
    l = len(new_columns)//4
    """if new_columns and all(col in source_data.columns for col in new_columns):
        for column_name in new_columns:
            source_data[column_name] = source_data[column_name].astype(str)"""

    for i in range(l):

        current_col = new_columns[4*i]
        append_af = new_columns[4*i + 2]
        append_bef = new_columns[4*i + 3]
        rename_col = new_columns[4*i + 1]
        
        if 'option1' in dropdown_options[i]:  # Upper Case
            source_data[current_col] = source_data[current_col].str.upper()

        if 'option2' in dropdown_options[i]:  # Lower Case
            source_data[current_col] = source_data[current_col].str.lower()

        if 'option3' in dropdown_options[i]:  # Capitalize
            source_data[current_col] = source_data[current_col].str.capitalize()


        source_data[current_col] = source_data[current_col] + ' ' + str(append_af)

        source_data[current_col] = str(append_bef) + ' ' + source_data[current_col]

        source_data.rename(columns={current_col: rename_col}, inplace=True)

    return source_data


# Step 3: Load - Append source data to target data
def load_data(source_data, target_data, output_file):
    merged_data = pd.concat([target_data, source_data], ignore_index=True)
    merged_data.to_csv(output_file, index=False)

# Step 4: Delete the source file
def delete_file(source_file):
    os.remove(source_file)

# ETL Pipeline
def etl_pipeline(source_file, target_file, output_file):
    # Extract data from source and target files
    source_data, target_data = extract_data(source_file, target_file)
    
    # Transform data (add constraints)
    source_data = transform_data(source_data, target_data)
    
    # Load data (append source data to target data)
    load_data(source_data, target_data, output_file)
    
    # Delete the source file
    delete_file(source_file)

# Example usage
if __name__ == "__main__":
    source_file = "source_data.csv"
    target_file = "target_data.csv"
    output_file = "merged_data.csv"
    
    etl_pipeline(source_file, target_file, output_file)