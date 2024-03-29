from pickle import load
from pprint import pprint
from random import sample

from cls_def import *
from s4_parse_sql import *
from repo_parse_sql import *
from parse_join_query import *
from sample import print_table_obj, print_query_obj, print_fk_obj


if __name__ == "__main__":
    coltype_freq_dict = dict()
    coltype_freq_dict["None"] = 0
    pickle_fpath = "data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_19_10:27:56/s4_parsed_sql_repo_list_2022_04_19_10:27:56.pkl"
    with open(pickle_fpath, "rb") as fp:
        repo_list = load(fp)
    # total_file_obj_count = 0
    not_empty_count = 0
    total_table_count = 0
    not_empty_table_count = 0
    query_count = 0
    have_fk_table_count = 0
    have_pk_table_count = 0
    have_key_table_count = 0
    total_pk_nums = 0
    total_fk_nums = 0
    total_index_nums = 0
    total_uniq_nums = 0
    total_key_nums = 0
    total_join_count = 0
    total_binary_join_count = 0
    total_condition_count = 0
    total_column_nums = 0
    have_type_column_nums = 0
    have_projection_query_nums = 0
    have_aggregation_query_nums = 0
    have_selection_query_nums = 0
    have_groupby_query_nums = 0
    # repo_list = sample(repo_list, 100)
    for i, repo in enumerate(repo_list):
        # filter empty table object
        if repo is None or (len(repo.name2tab) == 0 and len(repo.join_query_list) == 0):
            continue
        print('-' * 120)
        print(f"repo:{i+1}")
        print(repo.repo_url)
        not_empty_count += 1
        # total_file_obj_count += len(repo.parsed_file_list)

        # if(not_empty_count % 100 == 0):
        # print(not_empty_count)
        """
        if not_empty_table_count > 1000:
            break
        """
        query_count += len(repo.join_query_list)
        # for case in repo.check_failed_cases:
        # print(case)
        # """
        # """
        for query_obj in repo.join_query_list:
            if query_obj.projection_dict:
                have_projection_query_nums += 1
                print(f"projection dict: {query_obj.projection_dict}")
            if query_obj.aggregate_dict:
                have_aggregation_query_nums += 1
                print(f"aggregate dict: {query_obj.aggregate_dict}")
            if query_obj.selection_dict:
                have_selection_query_nums += 1
                print(f"selection dict: {query_obj.selection_dict}")
            if query_obj.groupby_dict:
                have_groupby_query_nums += 1
                print(f"groupby dict: {query_obj.groupby_dict}")
            # print_query_obj(query_obj)
            total_binary_join_count += len(query_obj)
            if query_obj.binary_joins:
                for binary_join in query_obj.binary_joins:
                    total_condition_count += len(query_obj.binary_joins)
        # """
        for table_name, table_object in repo.name2tab.items():
            total_table_count += 1
            if len(table_object.name2col) != 0:
                total_column_nums += len(table_object.name2col)
                for cname, cobj in table_object.name2col.items():
                    if cobj.col_type is not None:
                        have_type_column_nums += 1
                        if cobj.col_type not in coltype_freq_dict:
                            coltype_freq_dict[cobj.col_type] = 0
                        coltype_freq_dict[cobj.col_type] += 1
                    else:
                        coltype_freq_dict["None"] += 1
                        # print(table_name)
                print_table_obj(table_object)
                print()
                # print(table_object.name2col)
                not_empty_table_count += 1
            # """
    # print(f"{}")
    # print("avg binary join in each query:", total_binary_join_count / query_count)
    # print("avg condition in each query:", total_condition_count / query_count)
    # """
            if table_object.fk_list:
                print()
                for fk in table_object.fk_list:
                    total_fk_nums += 1
                    # print(f"fk_cols: {fk.fk_cols} | ref_table: {fk.ref_tab} | ref_cols: {fk.ref_cols}")
                    # print_fk_obj(table_object.tab_name, fk)
                    # print()
                # have_fk_table_count += 1
            if table_object.key_list:
                print()
                for key in table_object.key_list:
                    if key.key_type == "PrimaryKey":
                        total_pk_nums += 1
                        # have_pk_table_count += 1
                    elif key.key_type == "CandidateKey":
                        total_key_nums += 1
                    elif "Unique" in key.key_type:
                        total_uniq_nums += 1
                    # print(f"key_type: {key.key_type} | key_col_list: {key.key_col_list}")
                # have_key_table_count += 1
            if table_object.index_list:
                print()
                for idx in table_object.index_list:
                    total_index_nums += 1
            print()
            print()
    # """
    # print(f"table_name: {table_name}")
    # if table_object.fk_list:
    # print(repo.repo_url)
    # for fk in table_object.fk_list:
    # print_fk_obj(fk)
    # """
    # pprint(table_object.fk_list)  # print fk list in table
    # pprint(table_object.key_list)  # print key list in table
    # pprint(table_object.name2col)  # print column list in table
    print(f"Totally not empty repo: {not_empty_count}")
    # print(f"Totally file object nums: {total_file_obj_count}")
    print(f"Totally table count: {total_table_count}")
    print(f"Totally not empty table: {not_empty_table_count}")
    print(f"Totally query count: {query_count}")
    print(f"Totally have projection query nums: {have_projection_query_nums}")
    print(f"Totally have aggregation query nums: {have_aggregation_query_nums}")
    print(f"Totally have selection query nums: {have_selection_query_nums}")
    print(f"Totally have groupby query nums: {have_groupby_query_nums}")
    # print(f"Totally have pk table nums: {have_pk_table_count}")
    # print(f"Totally have fk table nums: {have_fk_table_count}")
    # print(f"Totally have key table nums: {have_key_table_count}")
    print(f"Totally pk nums: {total_pk_nums}")
    print(f"Totally fk nums: {total_fk_nums}")
    print(f"Totally key nums: {total_key_nums}")
    print(f"Totally index nums: {total_index_nums}")
    print(f"Totally unique nums: {total_uniq_nums}")
    print(f"Totally column nums: {total_column_nums}")
    print(f"Totally column nums with type: {have_type_column_nums}")
    print()

    # coltype_freq_dict = {k: v for k, v in sorted(coltype_freq_dict.items(), key=lambda item: item[1], reverse=True)}
    # for col, freq in coltype_freq_dict.items():
    # print(col, freq)
