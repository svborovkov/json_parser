import json
import pandas as pd
import os
import os.path
import re
import json_parser


# Train: getting info from two documents
def extract_features(extracted_data, additional_columns_info, simplified_arrays):
    branches_df_name = "root/branches/"
    if not simplified_arrays:
        branches_df_name += "_array_/"

    additional_columns_info = additional_columns_info.copy()
    features_df = extracted_data['root/'][['filename']].copy()
    branches_df = extracted_data[branches_df_name]
    city_counts = branches_df.groupby(
        ['filename', 'city']).size().reset_index(name='city_count')
    city_counts['city'] = 'branch_count_' + city_counts['city'].astype(str)
    city_counts = city_counts.pivot(
        index='filename', columns='city', values='city_count').reset_index()

    json_parser.add_columns_to_df(
        city_counts, additional_columns_info[additional_columns_info['dataframe'] == 'city_count'])
    additional_columns_info = json_parser.get_df_columns_info_df(
        city_counts, "city_count", additional_columns_info)
    city_counts = city_counts.fillna(0).astype(
        {col: int for col in city_counts.columns if col != 'filename'})
    features_df = features_df.merge(city_counts, on='filename', how='left')
    return features_df, additional_columns_info


# read files from "train" folder
simplified_arrays = False

if (simplified_arrays):
    train_dir = "train_simplified_arrays"
else:
    train_dir = "train"

json_dict = {}
for f in os.listdir(train_dir):
    full_fn = os.path.join(train_dir, f)
    if os.path.isfile(full_fn):
        with open(full_fn, "r", encoding="UTF8") as fd:
            json_str = fd.read()
        json_dict[full_fn] = json_str

# init parser object
parser = json_parser.json_parser(simplified_arrays = simplified_arrays)

# extract info about arrays
repeat_nodes = parser.extract_repeat_nodes(json_dict)

# Training step: extract all the data to set of dataframes and information about columns
extracted_data, columns_info = parser.extract_data(
    json_dict, "filename", repeat_nodes)

# save information about columns to dataframes_columns.csv file.
columns_info.to_csv(f"{train_dir}\\result\\dataframes_columns.csv",
                    index=False, sep=",")

# save dataframes to see what is stored in dataframes.
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv(f"{train_dir}\\result\\" + file_name+".csv",
              index=False, sep=",", encoding="UTF8")


# extracting features + getting additional columns info
additional_columns_info = pd.DataFrame(
    columns=["col", "type", "dataframe"], dtype=str)
features, additional_columns_info = extract_features(
    extracted_data, additional_columns_info, simplified_arrays)

additional_columns_info.to_csv(f"{train_dir}\\result\\additional_columns_info.csv",
                               index=False, sep=",")
features.to_csv(f"{train_dir}\\result\\features.csv", index=False, sep=",")

# now we can use data from previous step to extract data from similar json files:


# processing "test_example/seb.json"

# read information about columns from dataframes_columns.csv
columns_info = pd.read_csv(f"{train_dir}\\result\\dataframes_columns.csv", sep=",")

# read another json file with the same structure but some absent nodes
with open("test_example\\seb.json", "r", encoding="UTF8") as fd:
    json_str = fd.read()
json_dict = {"seb.json": json_str}

# reinitialize parser object
parser = json_parser.json_parser(simplified_arrays = simplified_arrays)

# data extraction
extracted_data, new_columns_info = parser.extract_data(
    json_dict, "filename", columns_info=columns_info)

# saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_seb\\" +
              file_name+".csv", index=False, sep=",")

features, temp_df = extract_features(
    extracted_data, additional_columns_info, simplified_arrays)
# as we can see extract_features() finishes without errors because extract_data recreated dataframe with branches
# and extract_features() recreated branch_count_* columns.

features.to_csv("test_example\\results_seb\\features.csv",
                index=False, sep=",")


# processing test_example/luminor.json

# read information about columns from dataframes_columns.csv
columns_info = pd.read_csv(f"{train_dir}\\result\\dataframes_columns.csv", sep=",")

# read another json file with the same structure but some absent nodes
with open("test_example\\luminor.json", "r", encoding="UTF8") as fd:
    json_str = fd.read()
json_dict = {"luminor.json": json_str}

# reinitialize parser object
parser = json_parser.json_parser(simplified_arrays = simplified_arrays)

# data extraction
extracted_data, new_columns_info = parser.extract_data(
    json_dict, "filename", columns_info=columns_info)
# We can see warning messages because new arrays exist in json file. Is we add this file to train set there will be additional dataframe.

# saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_luminor\\" +
              file_name+".csv", index=False, sep=",")

features, temp_df = extract_features(extracted_data, additional_columns_info, simplified_arrays)

features.to_csv("test_example\\results_luminor\\features.csv",
                index=False, sep=",")


# processing two json files from test_example

# read information about columns from dataframes_columns.csv
columns_info = pd.read_csv(f"{train_dir}\\result\\dataframes_columns.csv", sep=",")

# read another json file with the same structure but some absent nodes
json_dict = {}
dir = "test_example"
for f in os.listdir(dir):
    full_fn = os.path.join(dir, f)
    if os.path.isfile(full_fn):
        with open(full_fn, "r", encoding="UTF8") as fd:
            json_str = fd.read()
        json_dict[full_fn] = json_str

# reinitialize parser object
parser = json_parser.json_parser(simplified_arrays = simplified_arrays)

# data extraction
extracted_data, new_columns_info = parser.extract_data(
    json_dict, "filename", columns_info=columns_info)
# We can see warning messages because new arrays exist in json file. Is we add this file to train set there will be additional dataframe.

# saving dataframes
for df_name, df in extracted_data.items():
    file_name = re.sub('[^0-9a-zA-Z]', '_', df_name)
    df.to_csv("test_example\\results_both\\" +
              file_name+".csv", index=False, sep=",")

features, temp_df = extract_features(extracted_data, additional_columns_info, simplified_arrays)

features.to_csv("test_example\\results_both\\features.csv",
                index=False, sep=",")
