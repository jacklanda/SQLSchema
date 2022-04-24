from pickle import load
from pprint import pprint
from random import sample

from cls_def import *
from s4_parse_sql import *
from repo_parse_sql import *
from parse_join_query import *
from sample import print_table_obj, print_query_obj, print_fk_obj
import utils

# effectively replaces the print_multiline_lm() func from orig code
if __name__ == "__main__":
    #pickle_fpath = "/datadrive/yang/exp/data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_12_08:17:37/s4_parsed_sql_repo_list_2022_04_12_08:17:37.pkl"
    #pickle_fpath = "/datadrive/yang/exp/data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_08_15:07:33/s4_parsed_sql_repo_list_2022_04_08_15:07:33.pkl"
    pickle_fpath = "/datadrive/yang/exp/data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_08_15:07:33/s4_parsed_sql_repo_list_2022_04_08_15:07:33_8.pkl"
    #pickle_fpath = "/datadrive/yang/exp/data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_11_13:10:55/s4_parsed_sql_repo_list_2022_04_11_13:10:55.pkl"
    output_csv = os.path.join('/datadrive/yeye', pickle_fpath.split('/')[-1] + ".csv")
    with open(output_csv, "w") as writer:
        with open(pickle_fpath, "rb") as fp:
            repo_list = load(fp)
        for i, repo in enumerate(repo_list):
            # filter empty table object
            if repo is None or (len(repo.name2tab) == 0 and len(repo.join_query_list) == 0):
                continue


            if(i % 1000 == 0):
                print (i)
                
            for table_name in repo.name2tab:
                table_object =  repo.name2tab[table_name]
                lines = table_object.print_for_lm_multi_line()
                
                for l in lines:
                    writer.write(l + '\n')

