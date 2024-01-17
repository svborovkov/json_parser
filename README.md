json_parser is written in Python and it can extract data from similar json documents to set of dataframes.
It can be useful in can you need to analyze and extract data from jsons you got from some API.

Structure of documents can be a bit different - for example if some node exists only for 1 file of 100, this parser will extract all the data.

Extraction to multiple dataframes helps with analysis.

Child dataframes have links to paren ones.

Json_parser can fit with your json files and then recreate fully structure of dataframes even if you parse only one small, almost empty json, so its easy to write simple and safe code, because all dataframes and columns will exist.

Look at "parser.py" to understand how it works. There is an example how to train json on 2 files from `train` foilder. 
In "train" step it extracts data and saves info about structure to `train\result\dataframes_columns.csv`. 
Extracted data to dataframes is stored in other csv files in the folde `train\result\`

Afterwards, json_parser uses `train\result\dataframes_columns.csv` to extract data from two other files (folder `test_example`)


Unit tests are located in `test_json_parser.py`. They use folder `test_files`
