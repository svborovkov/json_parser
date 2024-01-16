import json
import pandas as pd

class json_parser:

    def extract_repeat_nodes(self, jsons_dict):
        repeat_nodes = set([])
        for json_doc in jsons_dict.values():
            self.__erp_process_element(json.loads(json_doc), "root/", repeat_nodes)
        repeat_nodes = list(repeat_nodes)
        repeat_nodes.sort()
        repeat_nodes.insert(0, "root/")
        return repeat_nodes

    def extract_data(self, jsons_dict, document_id_column_name, repeat_nodes = None, columns_info = None):
        if not repeat_nodes is None and columns_info is None:
            pass
        elif repeat_nodes is None and not columns_info is None:
            repeat_nodes = columns_info.dataframe.unique()
        else:
            raise Exception("Please provide only one of parameters either repeat_nodes or columns_info")

        extracted_data = {node_name: [] for node_name in repeat_nodes}

        for id, json_str in jsons_dict.items():
            parsed_json = json.loads(json_str)
            initial_ids_dict = {document_id_column_name:id}
            extracted_data['root/'].append(initial_ids_dict.copy()) # we will change it later. So we shouldn't have a link to ids_dict
            self.__ed_process_node(node=parsed_json,
                                                    current_repeat_node="root/",
                                                    current_path_in_repeat_node="",
                                                    current_data=extracted_data,
                                                    ids_dict=initial_ids_dict) 

        extracted_data_dfs = {name: pd.DataFrame.from_records(data) for name, data in extracted_data.items()}

        dfs_info = pd.DataFrame()
        for df_name in extracted_data_dfs.keys():
            col_info = extracted_data_dfs[df_name].dtypes.apply(lambda x: x.name).to_dict()
            columns_info_df = pd.DataFrame(col_info.items(), columns = ["col", "type"]).assign(dataframe = df_name)
            dfs_info = pd.concat([dfs_info, columns_info_df])

        if not columns_info is None:
            for i in columns_info.index:
                row = columns_info.iloc[i]
                column_name = row['col']
                df = extracted_data_dfs[row['dataframe']]
                if not column_name in df.columns:
                    df[column_name] = pd.Series([], dtype = row['type'])
        return extracted_data_dfs, dfs_info.reset_index(drop = True)

    def __erp_process_element(self, node, current_path, repeat_nodes):
        if isinstance(node, list):
            repeat_nodes.add(current_path + "_array_/")
            self.__erp_process_list(node, current_path, repeat_nodes)
        elif isinstance(node, dict):
            self.__erp_process_dict(node, current_path, repeat_nodes)
        # else - just value. We shouldn't care about them while we a researching for repeat nodes.
        # else:
        #     raise Exception("__erp_process_element shoule be executed only with list or dict")
        return

    def __erp_process_dict(self, dct, current_path, repeat_nodes):
        for name, value in dct.items():
            self.__erp_process_element(value, current_path + name + "/", repeat_nodes)
        return

    def __erp_process_list(self, lst, current_path, repeat_nodes):
        for value in lst:
            self.__erp_process_element(value, current_path + "_array_/", repeat_nodes)
        return

    def __ed_process_node(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict):
        if isinstance(node, list):
            self.__ed_process_list(node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict)
        elif isinstance(node, dict):
            self.__ed_process_dict(node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict)
        else:
            raise Exception("ERROR. Now simple data shoule be read by dict or list functions", current_repeat_node + current_path_in_repeat_node, " type:", type(node), " ", node)
        return

    def __ed_process_list(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict):
        new_repeat_path = current_repeat_node + current_path_in_repeat_node + "/_array_/"
        if not new_repeat_path in current_data:
            print("Warning! New array at path:", new_repeat_path, ", data is ignored. Info:", ids_dict)
        else:
            for index, subnode in enumerate(node):
                new_ids_dict = ids_dict.copy()
                new_ids_dict[new_repeat_path] = index
                current_data[new_repeat_path].append(new_ids_dict.copy()) # we will change it later. So we shouldn't have a link to ids_dict
                if isinstance(subnode, (list, dict)):
                    self.__ed_process_node(node = subnode,
                                                          current_repeat_node = new_repeat_path,
                                                          current_path_in_repeat_node = "",
                                                          current_data = current_data,
                                                          ids_dict = new_ids_dict)
                else: # value
                    current_data[new_repeat_path][-1].update({'unnamed_value': subnode})
        return

    def __ed_process_dict(self, node, current_repeat_node, current_path_in_repeat_node, current_data, ids_dict):
        for name, subnode in node.items():
            if current_path_in_repeat_node != "":
                new_path_repeat_node = current_path_in_repeat_node + "/" + name
            else:
                new_path_repeat_node = name
            if isinstance(subnode, (list, dict)):
                self.__ed_process_node(node = subnode,
                                                      current_repeat_node = current_repeat_node,
                                                      current_path_in_repeat_node = new_path_repeat_node,
                                                      current_data = current_data,
                                                      ids_dict = ids_dict)
            else: #value
                current_data[current_repeat_node][-1].update({new_path_repeat_node: subnode})
        return
