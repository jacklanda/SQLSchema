from pickle import load
from pprint import pprint
from random import sample, choice

from repo_parse_sql import Repository

# 对每个user，合并所有repo的name2tab，把合并结果记录到一个哈希表中<repo_user:name2tab>
# 对每个repo，遍历同一user下的每个unfound table，如果table 不出现在所属的repo内但出现在user下的repo内
# 则认为该repo的table在同一用户的其它repo下出现过。


def get_name2tab_same_user(repo_list):
    d = dict()
    for repo in repo_list:
        repo_user = repo.repo_url.rsplit('/', 1)[0].rsplit('/', 1)[1]
        name2tab = repo.name2tab
        """
        for table in repo.unfound_tables:
            if repo_user not in d:
                d[repo_user] = list()
            d[repo_user].append(table)
        """
        if repo_user not in d:
            d[repo_user] = dict()
        d[repo_user] |= name2tab
    return d


def calc(repo_list):
    total_table_num, in_other_repo_table_num = 0, 0
    d = get_name2tab_same_user(repo_list)
    for repo in repo_list:
        name2tab = repo.name2tab
        repo_user = repo.repo_url.rsplit('/', 1)[0].rsplit('/', 1)[1]
        for table in repo.unfound_tables:
            print(table)
            if table not in name2tab and table in d[repo_user]:
                in_other_repo_table_num += 1
            """
            if d[repo_user].count(table) > 1:
                in_other_repo_table_num += 1
            """
            total_table_num += 1
    print(f"found table in other repo: {in_other_repo_table_num/total_table_num}({in_other_repo_table_num}/{total_table_num})")


def calc_failed_cases_num(repo_list):
    table_failed_num, column_failed_num = 0, 0
    total_failed_cases_num = 0
    for repo in repo_list:
        for fp, fp_case in repo.check_failed_cases:
            total_failed_cases_num += len(fp_case)
            for case in fp_case:
                if "failed on check table" in case[1]:
                    table_failed_num += 1
                elif "failed on check column" in case[1]:
                    column_failed_num += 1
    print("total failed cases num:", total_failed_cases_num)
    print("total table check failed cases num:", table_failed_num)
    print("total column check failed cases num:", column_failed_num)


if __name__ == "__main__":
    # fpath = "data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_03_15_14:14:47/s4_parsed_sql_repo_list_2022_03_15_14:14:47.pkl"
    # repo_list = load(open(fpath, "rb"))
    # repo_list = [r for r in repo_list if r.unfound_tables]
    # calc(repo_list)
    # exit()

    fpath = "data/s4_sql_files_parsed/s4_parsed_sql_repo_list_2022_03_21_07:12:24/s4_parsed_sql_repo_list_2022_03_21_07:12:24.pkl"
    repo_list = load(open(fpath, "rb"))
    repo_list = [r for r in repo_list if r.check_failed_cases]
    # calc_failed_cases_num(repo_list)
    # exit()

    samples = sample(repo_list, 600)
    check_table_failed_num, check_column_failed_num = 0, 0
    for repo in samples:
        fp, fp_case = choice(repo.check_failed_cases)
        case = choice(fp_case)
        case_type = case[1]
        if "failed on check table" in case_type and check_table_failed_num < 100:
            check_table_failed_num += 1
            print(repo.repo_url)
            print(fp)
            pprint(case)
            print()
        if "failed on check column" in case_type and check_column_failed_num < 100:
            check_column_failed_num += 1
            print(repo.repo_url)
            print(fp)
            pprint(case)
            print()
        if check_table_failed_num == 100 and check_column_failed_num == 100:
            break
        # exit()
    """
    # sample parse result
    query_list = load(open("query_parse_result.pkl", "rb"))
    query_list = sample(query_list, 200)
    for query_stmt, query_obj in query_list:
        print('-' * 120)
        print(query_stmt)
        print()
        print(query_obj)
    """
