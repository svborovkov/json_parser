import json
import pandas as pd
import os
import os.path
import re
from json_parser import json_parser


#Train: getting info from two documents

# read files from "train" folder
json_dict = {}
dir = "train"
for f in os.listdir(dir):
    full_fn = os.path.join(dir, f)
    if os.path.isfile(full_fn):
        with open(full_fn, "r", encoding = "UTF8") as fd:
            json_str = fd.read()
        json_dict[full_fn] = json_str

# init parser object
parser = json_parser()

# extract info about arrays
repeat_nodes = parser.extract_repeat_nodes(json_dict)

# Training step: extract all the data to set of dataframes and information about columns
extracted_data, columns_info = parser.extract_data(json_dict, "filename", repeat_nodes)

# save information about columns to dataframes_columns.csv file.
columns_info.to_csv("train\\result\\dataframes_columns.csv", index = False, sep=";")

# save dataframes to see what is stored in dataframes.
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("train\\result\\"+ file_name+".csv", index = False, sep=";", encoding = "UTF8")



# now we can use data from previous step to extract data from similar json files:


# processing "test_example/seb.json"
    
# read information about columns from dataframes_columns.csv 
columns_info = pd.read_csv("train\\result\\dataframes_columns.csv", sep=";")

# read another json file with the same structure but some absent nodes
with open("test_example\\seb.json", "r", encoding = "UTF8") as fd:
    json_str = fd.read()
json_dict = {"seb.json":json_str}

# reinitialize parser object
parser = json_parser()

#data extraction 
extracted_data, new_columns_info = parser.extract_data(json_dict, "filename", columns_info = columns_info)

#saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_seb\\" + file_name+".csv", index = False, sep=";")


# processing test_example/luminor.json
    
# read information about columns from dataframes_columns.csv 
columns_info = pd.read_csv("train\\result\\dataframes_columns.csv", sep=";")

# read another json file with the same structure but some absent nodes
with open("test_example\\luminor.json", "r", encoding = "UTF8") as fd:
    json_str = fd.read()
json_dict = {"luminor.json":json_str}

# reinitialize parser object
parser = json_parser()

#data extraction 
extracted_data, new_columns_info = parser.extract_data(json_dict, "filename", columns_info = columns_info)
# We can see warning messages because new arrays exist in json file. Is we add this file to train set there will be additional dataframe.

#saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_luminor\\" + file_name+".csv", index = False, sep=";")



# processing two json files from test_example 

# read information about columns from dataframes_columns.csv 
columns_info = pd.read_csv("train\\result\\dataframes_columns.csv", sep=";")

# read another json file with the same structure but some absent nodes
json_dict = {}
dir = "test_example"
for f in os.listdir(dir):
    full_fn = os.path.join(dir, f)
    if os.path.isfile(full_fn):
        with open(full_fn, "r", encoding = "UTF8") as fd:
            json_str = fd.read()
        json_dict[full_fn] = json_str

# reinitialize parser object
parser = json_parser()

#data extraction 
extracted_data, new_columns_info = parser.extract_data(json_dict, "filename", columns_info = columns_info)
# We can see warning messages because new arrays exist in json file. Is we add this file to train set there will be additional dataframe.

#saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_both\\" + file_name+".csv", index = False, sep=";")


