from flask import Flask, request, render_template, redirect, url_for, send_file
import pandas as pd
import os
from test import extract_data, transform_data, load_data

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('new.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        source_file = request.files['sourceFile']
        target_file = request.files['targetFile']

        if not (source_file and target_file):
            raise ValueError("Please upload both source and target CSV files.")

        source_path = os.path.join('uploads', source_file.filename)
        target_path = os.path.join('uploads', target_file.filename)

        source_file.save(source_path)
        target_file.save(target_path)

        # Retrieve additional options from the form
        new_columns = request.form.getlist('textInput')
        dropdown_options = request.form.getlist('dropdownOptions')

        # Use the retrieved options as needed
        print(f'New Columns: {new_columns}')
        print(f'Dropdown Options: {dropdown_options}')

        # Define the output file path
        output_file = "merged_data.csv"

        # Call the ETL function with source_file, target_file, and output_file
        etl_pipeline(source_path, target_path, output_file, new_columns, dropdown_options)

        # Delete uploaded files after processing
        os.remove(source_path)
        os.remove(target_path)

        return redirect(url_for('download_merged'))
    
    except Exception as e:
        return f"Error during file upload: {str(e)}"

@app.route('/download_merged')
def download_merged():
    try:
        # Define the path to the merged file
        merged_file_path = "merged_data.csv"

        # Check if the file exists before attempting to send it
        if os.path.exists(merged_file_path):
            return send_file(merged_file_path, as_attachment=True)
        else:
            return "Merged file not found."
    except Exception as e:
        return f"Error during download: {str(e)}"

# Call the ETL function with source_file, target_file, and output_file
def etl_pipeline(source_file, target_file, output_file, new_columns=None, dropdown_options=None):
    try:
        # Extract data from source and target files
        source_data, target_data = extract_data(source_file, target_file)
        
        # Transform data (add constraints)
        source_data = transform_data(source_data, target_data, new_columns, None, None, dropdown_options)  # Adjust parameters based on your HTML
        
        # Load data (append source data to target data)
        load_data(source_data, target_data, output_file)
        
        # Delete the source file
        # delete_file(source_file)

        return "ETL process completed successfully!"

    except Exception as e:
        return f"Error during ETL process: {str(e)}"

if __name__ == "__main__":
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    
    app.run(debug=True)
