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
    

def get_twb_files(input_folder_path):
    twb_files = [os.path.join(input_folder_path, f) for f in os.listdir(input_folder_path) if f.endswith('.twb')]
    return twb_files