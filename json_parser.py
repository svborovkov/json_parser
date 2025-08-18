import json
import pandas as pd


def get_df_columns_info_df(df,
                           df_name,
                           current_columns_info=pd.DataFrame(columns=["col", "type", "dataframe"], dtype=str)):
    col_info = df.dtypes.apply(lambda x: x.name).to_dict()
    add_columns_info_df = pd.DataFrame(col_info.items(), columns=[
        "col", "type"]).assign(dataframe=df_name)
    columns_info_df = pd.concat(
        [current_columns_info, add_columns_info_df], ignore_index=True)
    columns_info_df = columns_info_df.drop_duplicates(
        subset=["col", "type", "dataframe"], keep="first")

    return columns_info_df


def add_columns_to_df(df, column_info_df):
    for i in column_info_df.index:
        row = column_info_df.loc[i]
        column_name = row['col']
        if not column_name in df.columns:
            df[column_name] = pd.Series([], dtype=row['type'])
    return


class json_parser:

    def __init__(self, simplified_arrays=True, ignore_empty_arrays=True, type_of_data="json"):
        """
        simplified_arrays - if True, then arrays will be detected even in case there is no "[ ]" in some json documents
            Also, dataframe names will not contain "_array_/" suffix.
            Warning: this mode doesn't support arrays in arrays - [[1,2,3], [4,5,6]] 
        If False, then arrays will be represented in dataframe with "_array_/" suffix. In this case, arrays should be
            represented in json documents with "[ ]" brackets.
        """
        self.simplified_arrays = simplified_arrays
        self.ignore_empty_arrays = ignore_empty_arrays
        self.type_of_data = type_of_data

    def extract_repeat_nodes(self, dict_with_data):
        repeat_nodes = set([])
        for raw_data in dict_with_data.values():
            match self.type_of_data:
                case"json":
                    parsed_data = json.loads(raw_data)
                case "raw":
                    parsed_data = raw_data
#                case "xml":
#                    parsed_data = xmltodict.parse(row_data)
                case _:
                    raise Exception("Unknown type of data: " + type_of_data)

            self.__erp_process_element(parsed_data, "root/", repeat_nodes)

        repeat_nodes = list(repeat_nodes)
        repeat_nodes.sort()
        repeat_nodes.insert(0, "root/")
        return repeat_nodes

    def extract_data(self, jsons_dict, document_id_column_name, repeat_nodes=None, columns_info=None):
        if not repeat_nodes is None and columns_info is None:
            pass
        elif repeat_nodes is None and not columns_info is None:
            repeat_nodes = columns_info.dataframe.unique()
        else:
            raise Exception(
                "Please provide only one of parameters either repeat_nodes or columns_info")

        extracted_data = {node_name: [] for node_name in repeat_nodes}

        for id, raw_data in jsons_dict.items():
            match self.type_of_data:
                case"json":
                    parsed_data = json.loads(raw_data)
                case "raw":
                    parsed_data = raw_data
#                case "xml":
#                    parsed_data = xmltodict.parse(row_data)
                case _:
                    raise Exception("Unknown type of data: " + type_of_data)
            initial_ids_dict = {document_id_column_name: id}
            # we will change it later. So we shouldn't have a link to ids_dict
            extracted_data['root/'].append(initial_ids_dict.copy())
            self.__ed_process_node(node=parsed_data,
                                   current_repeat_node="root/",
                                   current_path_in_repeat_node="",
                                   current_data=extracted_data,
                                   ids_dict=initial_ids_dict,
                                   repeat_nodes=repeat_nodes)

        extracted_data_dfs = {name: pd.DataFrame.from_records(
            data) for name, data in extracted_data.items()}

        dfs_info = pd.DataFrame()
        for df_name in extracted_data_dfs.keys():
            dfs_info = pd.concat([dfs_info,
                                  get_df_columns_info_df(extracted_data_dfs[df_name], df_name)])

        if not columns_info is None:
            for df_name in columns_info["dataframe"].unique():
                df_columns_filterted = columns_info[columns_info["dataframe"] == df_name]

                df = extracted_data_dfs[df_name]

                add_columns_to_df(df, df_columns_filterted)

                for i in df_columns_filterted.index:
                    row = df_columns_filterted.loc[i]
                    column_name = row['col']
                    if not column_name in df.columns:
                        df[column_name] = pd.Series([], dtype=row['type'])

        return extracted_data_dfs, dfs_info.reset_index(drop=True)

    def __erp_process_element(self, node, current_path, repeat_nodes):
        if isinstance(node, list):
            if self.ignore_empty_arrays and len(node)==0:
                pass
            else:
                name = current_path
                if not self.simplified_arrays:
                    name += "_array_/"
                repeat_nodes.add(name)
                self.__erp_process_list(node, current_path, repeat_nodes)
        elif isinstance(node, dict):
            self.__erp_process_dict(node, current_path, repeat_nodes)
        # else - just value. We shouldn't care about them while we a researching for repeat nodes.
        # else:
        #     raise Exception("__erp_process_element shoule be executed only with list or dict")
        return

    def __erp_process_dict(self, dct, current_path, repeat_nodes):
        for name, value in dct.items():
            self.__erp_process_element(
                value, current_path + name + "/", repeat_nodes)
        return

    def __erp_process_list(self, lst, current_path, repeat_nodes):
        path = current_path
        if not self.simplified_arrays:
            path += "_array_/"
        for value in lst:
            self.__erp_process_element(
                value, path, repeat_nodes)
        return

    def __ed_process_node(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes):
        if isinstance(node, list):
            if self.ignore_empty_arrays and len(node) == 0:
                return
            self.__ed_process_list(
                node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes)
        elif self.simplified_arrays and current_path_in_repeat_node != "" and current_repeat_node + current_path_in_repeat_node + "/" in repeat_nodes:
            # If simplified_arrays is True, we can consider usual data as elements of arrays
            self.__ed_process_list(
                [node], current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes)
        elif isinstance(node, dict):
            self.__ed_process_dict(
                node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes)
        else:
            raise Exception("ERROR. Now simple data shoule be read by dict or list functions",
                            current_repeat_node + current_path_in_repeat_node, " type:", type(node), " ", node)
        return

    def __ed_process_list(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes):
        new_repeat_path = current_repeat_node
        if current_path_in_repeat_node != "": # this if is required for array in array because current_path_in_repeat_node is empty
            new_repeat_path += current_path_in_repeat_node + "/"
        if not self.simplified_arrays:
            new_repeat_path += "_array_/"
        if not new_repeat_path in current_data:
            print("Warning! New array at path:", new_repeat_path,
                  ", data is ignored. Info:", ids_dict)
        else:
            for index, subnode in enumerate(node):
                new_ids_dict = ids_dict.copy()
                new_ids_dict[new_repeat_path] = index
                # we will change it later. So we shouldn't have a link to ids_dict
                current_data[new_repeat_path].append(new_ids_dict.copy())
                if isinstance(subnode, (list, dict)):
                    self.__ed_process_node(node=subnode,
                                           current_repeat_node=new_repeat_path,
                                           current_path_in_repeat_node="",
                                           current_data=current_data,
                                           ids_dict=new_ids_dict,
                                           repeat_nodes = repeat_nodes)
                else:  # value
                    current_data[new_repeat_path][-1].update(
                        {'unnamed_value': subnode})
        return

    def __ed_process_dict(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict, repeat_nodes):
        for name, subnode in node.items():
            if current_path_in_repeat_node != "":
                new_path_repeat_node = current_path_in_repeat_node + "/" + name
            else:
                new_path_repeat_node = name
            if isinstance(subnode, (list, dict)):
                self.__ed_process_node(node=subnode,
                                       current_repeat_node=current_repeat_node,
                                       current_path_in_repeat_node=new_path_repeat_node,
                                       current_data=current_data,
                                       ids_dict=ids_dict,
                                       repeat_nodes = repeat_nodes)
            elif self.simplified_arrays and current_repeat_node + current_path_in_repeat_node + name +"/" in repeat_nodes:
                self.__ed_process_node(node=[subnode], # imulate array
                                       current_repeat_node=current_repeat_node,
                                       current_path_in_repeat_node=new_path_repeat_node,
                                       current_data=current_data,
                                       ids_dict=ids_dict,
                                       repeat_nodes = repeat_nodes)
            else:  # value
                current_data[current_repeat_node][-1].update(
                    {new_path_repeat_node: subnode})
        return
