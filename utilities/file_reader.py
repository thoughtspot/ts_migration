import os
import pandas as pd
from .constants import MAPPING_FILES
from configs import configs
import logging
# Define the path to the folder containing the CSV files
logger = logging.getLogger(__name__)

def get_mapping_file(file_name=None):
    logger.info("Getting mapping files")
    folder_path = configs.configs_dict.get("mapping_files")
    files = os.listdir(folder_path)
    
    # Filter out the CSV files
    csv_files = [file for file in files if file.endswith('.csv')]

    # Dictionary to store DataFrames
    dataframes = {}

    # Loop through each CSV file and read it into a DataFrame
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        dataframes[file] = df

    if file_name == None:
        return dataframes
    return dataframes[file_name]
    

# def get_twb_files(input_folder_path):
#     twb_files = [os.path.join(input_folder_path, f) for f in os.listdir(input_folder_path) if f.endswith('.twb')]
#     return twb_files
import os
import zipfile

def get_twb_files(input_folder_path):
    twb_files = []
    
    # List all files in the directory
    for file_name in os.listdir(input_folder_path):
        full_path = os.path.join(input_folder_path, file_name)
        
        # Case 1: It's already a .twb
        if file_name.endswith('.twb'):
            twb_files.append(full_path)
            
        # Case 2: It's a .twbx that needs conversion
        elif file_name.endswith('.twbx'):
            try:
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    # Find the .twb file inside the package
                    internal_twb_names = [f for f in zip_ref.namelist() if f.endswith('.twb')]
                    
                    for internal_name in internal_twb_names:
                        # Extract the .twb file to the input folder
                        zip_ref.extract(internal_name, input_folder_path)
                        
                        # Construct the new path and add to our list
                        extracted_path = os.path.join(input_folder_path, internal_name)
                        twb_files.append(extracted_path)
                        
                        print(f"Converted: {file_name} -> {internal_name}")
            except Exception as e:
                print(f"Could not process {file_name}: {e}")
                
    return twb_files