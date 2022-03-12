import random
from pickle import load


def print_query_obj(query_obj):
    print()
    print(f"BinaryJoin nums: {len(query_obj.binary_joins)}")
    for binaryjoin_obj in query_obj.binary_joins:
        print()
        print(f"\ttable_a: {binaryjoin_obj.table_a.table_name}")
        print(f"\ttable_b: {binaryjoin_obj.table_b.table_name}")
        print(f"\tjoin_type: {binaryjoin_obj.join_type}")
        for i, c in enumerate(binaryjoin_obj.conditions):
            print(f"\tcondition {i}: {c[0].col_name} {c[1]} {c[2].col_name}")


def print_column_obj(column_obj):
    print(f"\tcolumn name: {column_obj.col_name}")


def print_fk_obj(fk_obj):
    print("fk object↓")
    print("fk columns:")
    for col_obj in fk_obj.fk_cols:
        print_column_obj(col_obj)
    print("fk referred table↓")
    print(f"\t{fk_obj.ref_tab.tab_name}")
    for col_obj in fk_obj.ref_cols:
        print_column_obj(col_obj)


def print_table_obj(table_obj):
    print(f"table name: {table_obj.tab_name}")
    print()
    print("table column↓")
    for col_obj in table_obj.name2col.items():
        print_column_obj(col_obj[1])
    print()
    if len(table_obj.key_list):
        print("key list↓")
        for key_obj in table_obj.key_list:
            print(f"\tkey type: {key_obj.key_type}")
            for key_column_obj in key_obj.key_col_list:
                print_column_obj(key_column_obj)
    if len(table_obj.fk_list) != 0:
        print()
        for fk_obj in table_obj.fk_list:
            print_fk_obj(fk_obj)


def print_repo_obj(repo_obj):
    print(f"repo_url: {repo_obj.repo_url}")
    print(f"repo's tables: {repo_obj.name2tab}")


def sample_create_table_from_pickle_file(fpath):
    repo_list = load(open(fpath, "rb"))
    repo_list = [r for r in repo_list if len(r.name2tab) >= 3]
    return random.sample(repo_list, 100)


if __name__ == "__main__":
    pickle_fpath = "data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_01_18_16:23:27.pkl"
    from repo_parse_sql import Repository
    random_repo_list = sample_create_table_from_pickle_file(pickle_fpath)[:100]
    random_table_list = list()
    total_table_dict = dict()
    for repo_obj in random_repo_list:
        random_table_list += random.sample(list(repo_obj.name2tab.items()), 1)
    # print(random_table_list)
    # exit()
    for table_obj in random_table_list:
        print('-' * 120)
        print_table_obj(table_obj[1])
        print()
