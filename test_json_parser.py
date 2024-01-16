import unittest
import pandas as pd
import pandas.testing as pd_testing
import os
import os.path
import re
import json_parser as json_parser_class

class Test(unittest.TestCase):
    # json_parser = json_parser_class.json_parser()

    def read_files(self, file_list):
        json_dict = {}
        for f in file_list:
            with open(f, "r", encoding = "UTF8") as fd:
                json_str = fd.read()
            json_dict[f] = json_str
        return json_dict


    def check_one_case_extract_repeat_nodes(self, file_list, expected_repeat_nodes):
        json_dict = self.read_files(file_list)

        parser = json_parser_class.json_parser()
        repeat_nodes = parser.extract_repeat_nodes(json_dict)

        self.assertEqual(repeat_nodes, expected_repeat_nodes)

    def test_0_extract_repeat_nodes(self):
        print("Start test extract_repeat_nodes()")

        self.check_one_case_extract_repeat_nodes(["test_files\\1.json", "test_files\\2.json"], 
                                    ["root/",
                                    "root/array_object/_array_/",
                                    "root/array_str1/_array_/",
                                    "root/object1/array_int1/_array_/"])

        self.check_one_case_extract_repeat_nodes(["test_files\\1.json"], 
                                    ["root/",
                                    "root/array_object/_array_/",
                                    "root/object1/array_int1/_array_/"])

        print("\nFinish extract_repeat_nodes() test\n")

    def check_one_case_data_extraction(self, file_list, repeat_nodes, columns_info_fn, test_result_folder):
        print("Started check_one_case_data_extraction()" + test_result_folder)
        json_dict = self.read_files(file_list)

        parser = json_parser_class.json_parser()
        if not repeat_nodes is None and columns_info_fn is None:
            extr_data, extr_columns_info = parser.extract_data(json_dict, "filename", repeat_nodes)
        elif repeat_nodes is None and not columns_info_fn is None:
            column_info = pd.read_csv(columns_info_fn, sep=";")
            extr_data, extr_columns_info = parser.extract_data(json_dict, "filename", None, column_info)
        else:
            raise Exception("use repeat_nodes or columns_info_fn")

        # uncomment next line to update results
        # extr_columns_info.to_csv(test_result_folder + "col_info.csv", index = False, sep=";", encoding = "UTF8") 
        expected_columns_info = pd.read_csv(test_result_folder + "col_info.csv", sep=";")
        pd_testing.assert_frame_equal(expected_columns_info, extr_columns_info)

        for key in extr_data.keys():
            file_name = test_result_folder + re.sub('[^0-9a-zA-Z]', '_', key) + ".csv"
            # uncomment next line to update results
            # extr_data[key].to_csv(file_name, index = False, sep=";", encoding = "UTF8") 
            expected_columns_info = pd.read_csv(file_name, sep=";")
            pd_testing.assert_frame_equal(extr_data[key].reset_index(drop = True), 
                                          expected_columns_info,
                                          check_dtype = False)

        print("Finished check_one_case_data_extraction()" + test_result_folder)

    def test_1_data_extraction_using_repeat_nodes(self):
        
        self.check_one_case_data_extraction(
            file_list = ["test_files\\1.json"],
            repeat_nodes = ["root/",
                            "root/array_object/_array_/",
                            "root/object1/array_int1/_array_/"],
            columns_info_fn = None,
            test_result_folder = "test_files\\1\\")

        self.check_one_case_data_extraction(
            file_list = ["test_files\\2.json"],
            repeat_nodes = ["root/",
                            "root/array_object/_array_/",
                            "root/array_str1/_array_/",
                            "root/object1/array_int1/_array_/"],
            columns_info_fn = None,
            test_result_folder = "test_files\\2\\")

        self.check_one_case_data_extraction(
            file_list = ["test_files\\1.json", "test_files\\2.json"],
            repeat_nodes = None,
            columns_info_fn = "test_files\\1_2_col_info.csv",
            test_result_folder = "test_files\\1_2\\")

        self.check_one_case_data_extraction(
            file_list = ["test_files\\3.json"],
            repeat_nodes = None,
            columns_info_fn = "test_files\\3_col_info.csv",
            test_result_folder = "test_files\\3\\")


if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()
