# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import os
import sys
import time
import glob
import pickle
from random import sample, shuffle
from multiprocessing import Manager, Pool
from concurrent.futures import TimeoutError

from pebble import ProcessPool

from utils import get_chunks
from s4_parse_sql import parse_repo_files


PARALLEL = True
INPUT_FOLDER = os.path.join(os.getcwd(), "data/s3_sql_files_crawled_all_vms")
OUTPUT_FOLDER = os.path.join(os.getcwd(), "data/s4_sql_files_parsed")


class Repository:
    """Construct a repo object for a GitHub repository,
    which containts a set of SQL files.

    Params
    ------
    - repo_url: str
    - sql_file_set: set[tuple[str, str]]
    - repo_memo: Optional[dict, None], default=None
    - parsed_file_list: Optional[dict, None], default=None
    - join_query_list: Optinal[list, None], default=None

    Attribs
    -------
    - repo_url: str
    - repo_fpath_list: list[str]
    - repo_furl_list: list[str]
    - memo: dict[str:tuple[str, str, str]]
    - parsed_file_list: list[File]
    - join_query_list: list[Query]
    - name2tab: dict[str:Table]

    Returns
    -------
    - a repo object
    """

    def __init__(
        self,
        repo_url,
        sql_file_set,
        repo_memo=None,
        parsed_file_list=None,
        join_query_list=None,
    ):
        # The referred table may not be found while handling, use a memo to keep it temporarily,
        # and at last traverse this unhandled referred table name according to the more complete repo object.
        # n.b. should treat the temporary unfound referred table as normal and mark it in memo!
        self.__repo_url = repo_url
        self.__repo_fplist = [f[0] for f in sql_file_set]
        self.__repo_furls = [f[1] for f in sql_file_set]
        self.__repo_memo = repo_memo
        self.__parsed_file_list = parsed_file_list
        self.__join_query_list = join_query_list
        self.__name2tab = dict()
        self.__check_failed_cases = list()
        self.__unfound_tables = list()

    @ property
    def repo_url(self):
        "Get attribute `repo_url`(read-only)"
        return self.__repo_url

    @ property
    def repo_fpath_list(self):
        "Get attribute `repo_fpath_list`(read-only)"
        return self.__repo_fplist

    @ property
    def repo_furl_list(self):
        "Get attribute `repo_furl_list`(read-only)"
        return self.__repo_furls

    @ property
    def memo(self):
        """Get attribute `memo`(read-write)
        Property `memo` to set and get the record of unresolved
        referred table name with its fields of a specific table.

        Params
        ------
        - for getter
            - None
        - for setter
            - record: tuple[str, str, str, str]
            (`file_name`, `table_name`, `ref_table_name`, `ref_cols_name`)

        Returns
        -------
        - for getter
            - dict[str:tuple[str, str, str]]
        - for setter
            - None
        """
        return self.__repo_memo

    @ memo.setter
    def memo(self, m):
        """Property memo setter method."""
        if not isinstance(m, dict):
            raise TypeError("Could only assign attrib `repo_memo` with a dict object!")
        self.__repo_memo = m

    @property
    def parsed_file_list(self):
        """Get attribute `parsed_file_list`(read-write)
        Property `parsed_file_list` to set and get parsed SQL file objects.

        Params
        ------
        - for getter
            - None
        - for setter
            - l: list

        Returns
        -------
        - for getter
            - list
        - for setter
            - None
        """
        return self.__parsed_file_list

    @parsed_file_list.setter
    def parsed_file_list(self, l):
        """Property parsed_file_list setter method."""
        if not isinstance(l, list):
            raise TypeError("Could only assign attrib `parsed_file_list` with a list object!")
        self.__parsed_file_list = l

    @property
    def join_query_list(self):
        """Get attribute `join_query_list`(read-write)
        Property `join_query_list` to set and get a list of Query objects.

        Params
        ------
        - for getter
            - None
        - for setter
            - l: list

        Returns
        -------
        - for getter
            - list
        - for setter
            - None
        """
        return self.__join_query_list

    @join_query_list.setter
    def join_query_list(self, l):
        """Property join_query_list setter method."""
        if not isinstance(l, list):
            raise TypeError("Could only assign attrib `join_query_list` with a list object!")
        self.__join_query_list = l

    @property
    def name2tab(self):
        """Get attribute `name2tab`(read-write)

        Params
        ------
        - None

        Returns
        -------
        - dict
        """
        return self.__name2tab

    @name2tab.setter
    def name2tab(self, d):
        if not isinstance(d, dict):
            raise TypeError("Could only assign attrib `name2tab` with a dict object!")
        self.__name2tab = d

    @property
    def check_failed_cases(self):
        return self.__check_failed_cases

    @check_failed_cases.setter
    def check_failed_cases(self, l):
        self.__check_failed_cases = l

    @property
    def unfound_tables(self):
        return self.__unfound_tables

    @unfound_tables.setter
    def unfound_tables(self, t):
        self.__unfound_tables = t

    def insert(self, element):
        # if isinstance(element, File):
        if self.__parsed_file_list is None:
            self.__parsed_file_list = list()
        self.__parsed_file_list.append(element)


def dump_repo_list(parsed_repo_list, pkl_dir, pkl_fname):
    """Dump parsed repo list to a local pickle file.

    Params
    ------
    - parsed_repo_list: list[Repository]
    - pickle_fname: str, default="s4_parsed_sql_repo_list.pkl"

    Returns
    -------
    - None
    """
    pkl_fpath = os.path.join(pkl_dir, pkl_fname)
    pickle.dump(parsed_repo_list, open(pkl_fpath, "wb"))


def make_dir(f_name_base):
    dir_name = OUTPUT_FOLDER + '/' + f_name_base
    try:
        os.mkdir(dir_name)
    except:
        print(f"dir {dir_name} exists")
    return dir_name


def merge_pkl_files(dir_name):
    merge_list = list()
    pkl_files = [f for f in glob.glob(os.path.join(dir_name, "*.pkl"))]
    for pkl_file in pkl_files:
        partial_list = pickle.load(open(pkl_file, "rb"))
        merge_list += partial_list
    pickle.dump(merge_list, open(dir_name + '/' + dir_name.rsplit('/', 1)[-1] + ".pkl", "wb"))


def aggregate(fpath="data/s2_sql_file_list.txt", max_repo_limit=9999999):
    """Aggregate all the SQL files under the same repository,
    and return a list of `repo databases` where we treat all tables in a repository as a database.

    Params
    ------
    - fpath: str, default="data/s2_sql_file_list.txt"
    - max_repo_num: int, default=9999999

    Returns
    -------
    - repo_list: list[Repository]
    """
    repo_list = list()
    """
    repo_dict = dict()
    with open(fpath, "r") as fp:
        for i, line in enumerate(fp.readlines()):
            if i > max_repo_limit:
                break
            fields = line.split('\t')
            sql_fpath = os.path.join(INPUT_FOLDER, fields[0] + ".sql")
            # TODO: check if the file exists or not.
            if not os.path.isfile(sql_fpath):
                continue
            repo_url = fields[1].split("/blob")[0]
            raw_sql_url = fields[2]
            sql_tuple = (sql_fpath, raw_sql_url)
            if repo_url in repo_dict:
                repo_dict[repo_url].add(sql_tuple)
            else:
                repo_dict[repo_url] = set()
                repo_dict[repo_url].add(sql_tuple)
    """

    """
    user_nums = list()
    repo_dict = pickle.load(open("data/samples/repo_dict.pkl", "rb"))
    for repo_url, file_set in repo_dict.items():
        if repo_url == "https://github.com/OdyseeTeam/commentron":
            repo_obj = Repository(repo_url, file_set)
            repo_list.append(repo_obj)
            break
        # repo in the same user
        repo_user = repo_url.rsplit('/', 1)[0].rsplit('/', 1)[1]
        if len(user_nums) == 1000 and repo_user not in user_nums:
            continue
        elif len(user_nums) == 1000 and repo_user in user_nums:
            repo_obj = Repository(repo_url, list(file_set))
            repo_list.append(repo_obj)
        elif len(user_nums) != 1000 and repo_user not in user_nums:
            user_nums.append(repo_user)
            repo_obj = Repository(repo_url, list(file_set))
            repo_list.append(repo_obj)
        elif len(user_nums) != 1000 and repo_user in user_nums:
            repo_obj = Repository(repo_url, list(file_set))
            repo_list.append(repo_obj)
        repo_obj = Repository(repo_url, list(file_set))
        repo_list.append(repo_obj)
    shuffle(repo_list)
    print(f"Totally aggregate repo nums: {len(repo_list)}")
    """

    # return sample(repo_list, 11000)
    # """
    # return repo_list
    # pickle_fpath = f"data/samples/s4_parsed_sql_repo_list_{time.strftime('%Y_%m_%d_%H:%M:%S')}.pkl"
    # pickle.dump(samples, open("data/samples/repo_list_11k.pkl", "wb"))
    # pickle.dump(sample(repo_list, 100), open(pickle_fpath, "wb"))
    return pickle.load(open("data/samples/repo_list_11k.pkl", "rb"))
    # repo_list = pickle.load(open("data/samples/repo_list_11k.pkl", "rb"))
    # samples = sample(repo_list, 1100)
    # return samples


if __name__ == "__main__":
    manager = Manager()
    result_obj_list = list()
    parsed_repo_list = list()
    # user_name2tab = manager.dict()
    repo_list = aggregate()
    sys.setrecursionlimit(100000000)

    batch_num = 0
    parsed_repo_nums = 0
    pkl_fname_base = f"s4_parsed_sql_repo_list_{time.strftime('%Y_%m_%d_%H:%M:%S')}"
    pkl_dir = make_dir(pkl_fname_base)

    if PARALLEL:
        def task_done(future):
            try:
                result_obj = future.result()
                result_obj_list.append(result_obj)
            except TimeoutError as error:
                print("Function took longer than %d seconds" % error.args[1])
            except Exception as error:
                print("Function raised %s" % error)

        for i, batch in enumerate(get_chunks(repo_list, 110000)):
            with ProcessPool(max_workers=32, max_tasks=64) as pool:
                for repo in batch:
                    future = pool.schedule(parse_repo_files, (repo,), timeout=600)
                    future.add_done_callback(task_done)
                # result_obj_list.clear()
                # print(f"parse a batch({len(batch)}) of repos done")
            results = [r for r in result_obj_list if r is not None]
            # dump_repo_list(results, pkl_dir, pkl_fname_base + ".pkl")
            dump_repo_list(results, pkl_dir, pkl_fname_base + '_' + str(i) + ".pkl")
            result_obj_list.clear()
        merge_pkl_files(pkl_dir)

        # merge_pkl_files(pkl_dir)
        """
        pool = Pool(32)
        for i, repo in enumerate(repo_list):
            # repo_user = repo.repo_url.rsplit('/', 1)[0].rsplit('/', 1)[1]
            # if repo_user not in user_name2tab:
            # user_name2tab[repo_user] = manager.dict()  # dict[user:dict[table_name:Table]]
            # result_obj = pool.apply_async(parse_repo_files, (repo, user_name2tab[repo_user]))
            result_obj = pool.apply_async(parse_repo_files, (repo, ))
            result_obj_list.append(result_obj)
            if i % 220000 == 0:
                batch_num += 1
                results = (result_obj.get() for result_obj in result_obj_list)
                parsed_repo_list = [r for r in results if r is not None]
                dump_repo_list(parsed_repo_list, pkl_dir, pkl_fname_base + '_' + str(batch_num) + ".pkl")
                result_obj_list.clear()
            elif i == len(repo_list) - 1:
                batch_num += 1
                results = (result_obj.get() for result_obj in result_obj_list)
                parsed_repo_list = [r for r in results if r is not None]
                dump_repo_list(parsed_repo_list, pkl_dir, pkl_fname_base + '_' + str(batch_num) + ".pkl")
                result_obj_list.clear()
        merge_pkl_files(pkl_dir)
        """
    else:
        shuffle(repo_list)
        print(f"Totally aggregate repo nums: {len(repo_list)}")
        for i, repo in enumerate(repo_list):
            print("=" * 30, f'repo:{i+1}', repo.repo_url, "=" * 30)
            # parsed_repo = parse_repo_files(repo, user_name2tab)
            parsed_repo = parse_repo_files(repo)
            """
            if parsed_repo is not None:
                print(parsed_repo.name2tab)
                print()
                print(parsed_repo.join_query_list)
                print()
            """
            """
            for query_obj in parsed_repo.join_query_list:
                print(query_obj.hashid)
                for join_obj in query_obj.binary_joins:
                    print()
                    print("join object ↓")
                    print(f"source table: {join_obj.source_table.tab_name} : {join_obj.source_table}", )
                    print(f"join table: {join_obj.join_table.tab_name} : {join_obj.join_table}", )
                    print("join type:", join_obj.join_type)
                    print("conditions:", join_obj.conditions)
                    print("extract from:", join_obj.extract_from)
            """
            # parsed_repo_list.append(parsed_repo)
            # dump_repo_list(parsed_repo_list, pkl_dir, pkl_fname_base + '_' + str(i) + ".pkl")
