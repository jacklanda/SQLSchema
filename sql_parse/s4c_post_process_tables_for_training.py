####################################################
# taken from orig s4c
####################################################

import os
import pickle
import random
from pickle import load
from pprint import pprint
from random import sample

from cls_def import *
from s4_parse_sql import *
from repo_parse_sql import *
from parse_join_query import *
from sample import print_table_obj, print_query_obj, print_fk_obj
import utils


DEBUG = False # debug=True, then we only check first 50K files
# how do we handle tables with weak/unreliable signals? e.g., a table with no constraint or all NOTNULL constraints can be skipped
SKIP_NO_CONSTRAINT_TABLE = False
SKIP_NOTNULL_TABLE_HIGHER_THAN_THRESHOLD = 1 # skip all tables whose not-null perc is higher than 0.8
DEDUP_IDENTICAL_TABLE = False
#INPUT_FOLDER = os.path.join(os.getcwd(), 's4_sql_files_parsed_test')
INPUT_FOLDER = '/datadrive/yang/exp/data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_04_12_08:17:37/'
OUTPUT_FOLDER = INPUT_FOLDER
#PKL_DATA_SIZE = '15MLines.300KFiles'
#PKL_DATA_SIZE = '100MLines.2300KFiles
PKL_DATA_SIZE = '14MAllTables.RepoLevelTableDeduped'

ouput_lm_csv_file = os.path.join(INPUT_FOLDER, 's4_parsed_sql_into_lm.{}.skip_no_constraint_tab_{}.skip_notnull_tab_higher_than_{}.dedup_tables_{}.csv'.format(PKL_DATA_SIZE, SKIP_NO_CONSTRAINT_TABLE, SKIP_NOTNULL_TABLE_HIGHER_THAN_THRESHOLD, DEDUP_IDENTICAL_TABLE))
ouput_lm_csv_file_stats = ouput_lm_csv_file + '.stats'

# table level stats
total_input_tables = 0
total_skipped_too_high_notnull_tables = 0 # tables skipped for having too many NOTNULL cols (e.g., 80%)
total_skipped_no_constraint_tables = 0 # tables skipped for not having any constraints 
total_dup_tables = 0 # tables deduped for having identical cols
total_final_output_tables = 0 # tables finally printed out for lm training

# col level stats, among tables finally produced tables
total_unique_cols = 0
total_notnull_cols = 0
total_na_cols = 0

input_pkl_file = 's4_parsed_sql_repo_list_2022_04_12_08:17:37_0.pkl'
### we want to customize lm, in terms of what table to show and not show (e.g., a table with no constraint is skipped, a table with all NOTNULL is skipped, etc.)
### for diff SKIP_NO_CONSTRAINT_TABLE/SKIP_ALL_NOTNULL_TABLE settings
with open(os.path.join(INPUT_FOLDER, input_pkl_file), 'rb') as f:
    repo_list = pickle.load(f)
    
    cnt = 0
    for repo in repo_list:
        
        
        # filter tables, based on whether they have constraints, or they are aggressively all-not-nulls
        tab_list_after_filter = []
        #for f_obj in repo.parsed_file_list:

        #for tab_name in repo.name2tab:
        for tab_name in repo.name2tab:
            cnt += 1
            if(cnt % 1000 == 0):
                print(cnt)
            if(DEBUG and cnt == 50000):
                break
        
            tab_obj = repo.name2tab[tab_name]
            total_input_tables += 1
            if(len(tab_obj._name2col) == 0):
                continue
            if(SKIP_NO_CONSTRAINT_TABLE and tab_obj.is_table_all_cols_no_constraint()):
                total_skipped_no_constraint_tables += 1
                continue
            if(tab_obj.calc_table_notnull_perc() > SKIP_NOTNULL_TABLE_HIGHER_THAN_THRESHOLD):
                total_skipped_too_high_notnull_tables += 1
                continue
            tab_list_after_filter.append(tab_obj)
            
        
# from all filtered tables, find duplicates
key_2_tab_list_dict = {}
for tab_obj in tab_list_after_filter:
    sorted_cols = sorted(tab_obj.col_name_seq)
    tab_col_name_concat_key = tab_obj.tab_name + ':' + '___'.join(sorted_cols)
    if(tab_col_name_concat_key not in key_2_tab_list_dict):
        key_2_tab_list_dict[tab_col_name_concat_key] = []
    key_2_tab_list_dict[tab_col_name_concat_key].append(tab_obj)

# sort all  key_2_tab_list_dict  by freq
key_2_tab_list_dict = {k: v for k, v in sorted(key_2_tab_list_dict.items(), reverse=True, key=lambda item: len(item[1]))}

## write to CSV,
with open(ouput_lm_csv_file, 'w') as csv_writer, open(ouput_lm_csv_file_stats, 'w') as stats_writer:
    all_tabs_to_print = []
    for k, v in key_2_tab_list_dict.items():
        stats_writer.write('****** new dup cluster ***** \n')
        stats_writer.write('dup_cnt={}, key={} \n'.format(len(v), k))
        
        # if dedup, pick first
        if(DEDUP_IDENTICAL_TABLE):
            tabs_to_print = v[:1]
            total_dup_tables += (len(v) - 1)
            total_final_output_tables += 1
        else:
            tabs_to_print = v
            total_final_output_tables += len(v)
            
        all_tabs_to_print += tabs_to_print
        
    # shuffle all_tabs_to_print, to make sure that things are randomized
    random.shuffle(all_tabs_to_print)
    
    # print
    for tab_obj in all_tabs_to_print:
        lines = tab_obj.print_for_lm_multi_line()
        for l in lines:
            csv_writer.write(l + '\n')
        
        total_notnull_cols += tab_obj.total_inferred_notnull_col_cnt()
        total_unique_cols += tab_obj.total_inferred_unique_col_cnt()
        total_na_cols += tab_obj.total_inferred_no_constraint_col_cnt()

    stats_writer.write('total_input_tables: {}, total_skipped_too_high_notnull_tables: {}, total_skipped_no_constraint_tables: {}, total_dup_tabls: {}, total_final_output_tables: {}'.format(total_input_tables, total_skipped_too_high_notnull_tables, total_skipped_no_constraint_tables, total_dup_tables, total_final_output_tables))
    print('total_input_tables: {}, total_skipped_too_high_notnull_tables: {}, total_skipped_no_constraint_tables: {}, total_dup_tabls: {}, total_final_output_tables: {}'.format(total_input_tables, total_skipped_too_high_notnull_tables, total_skipped_no_constraint_tables, total_dup_tables, total_final_output_tables))

    stats_writer.write('total_notnull_cols: {}, total_unique_cols: {}, total_na_cols: {}'.format(total_notnull_cols, total_unique_cols, total_na_cols))
    print('total_notnull_cols: {}, total_unique_cols: {}, total_na_cols: {}'.format(total_notnull_cols, total_unique_cols, total_na_cols))
    

