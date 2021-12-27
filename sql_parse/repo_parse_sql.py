# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import os
import pickle

from s4_parse_sql import parse_repo_files


INPUT_FOLDER = os.path.join(os.getcwd(), "data/s3_sql_files_crawled_all_vms")
OUTPUT_FOLDER = os.path.join(os.getcwd(), "data/s4_sql_files_parsed")


class Repository:
    """Construct a repo object for a real GitHub repository,
    which containts a set of SQL files.

    Params
    ------
    - repo_url: str
    - sql_file_set: set[tuple[str, str]]
    - repo_memo: Optional[dict, None], default=None
    - parsed_file_list: Optional[dict, None], default=None

    Attribs
    -------
    - repo_url: str
    - repo_fpath_list: list[str]
    - repo_furl_list: list[str]

    Returns
    -------
    - a repo object
    """

    def __init__(self,
                 repo_url,
                 sql_file_set,
                 repo_memo=None,
                 parsed_file_list=None
                 ):
        # The referred table may not be found while handling, use a memo to keep it temporarily,
        # and at last traverse this unhandled referred table name according to the more complete repo object.
        # n.b. should treat the temporary unfound referred table as normal and mark it in memo!
        self.__repo_url = repo_url
        self.__repo_fplist = [f[0] for f in sql_file_set]
        self.__repo_furls = [f[1] for f in sql_file_set]
        self.__repo_memo = repo_memo
        self.__parsed_file_list = parsed_file_list
        self.__name2tab = dict()

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

    def insert(self, element):
        # if isinstance(element, File):
        if self.__parsed_file_list is None:
            self.__parsed_file_list = list()
        self.__parsed_file_list.append(element)


def dump_repo_list(parsed_repo_list, pickle_fname="s4_parsed_sql_repo_list.pkl"):
    """Dump parsed repo list to a local pickle file.

    Params
    ------
    - parsed_repo_list: list[Repository]
    - pickle_fname: str, default="s4_parsed_sql_repo_list.pkl"

    Returns
    -------
    - None
    """
    pickle_fpath = os.path.join(OUTPUT_FOLDER, pickle_fname)
    pickle.dump(parsed_repo_list, open(pickle_fpath, "wb"))


def aggregate(fpath="data/s2_sql_file_list_2021-11-11.txt", max_repo_limit=9999999):
    """Aggregate all the SQL files under the same repository,
    and return a list of `repo databases` where we treat all tables in a repository as a database.

    Params
    ------
    - fpath: str, default="data/s2_sql_file_list_2021-11-11.txt"
    - max_repo_num: int, default=9999999

    Returns
    -------
    - repo_list: list[Repository]
    """
    repo_dict = dict()
    repo_list = list()
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

    for repo_url, file_set in repo_dict.items():
        # if repo_url == "https://github.com/prelegalwonder/zabbix":
        # repo_obj = Repository(repo_url, file_set)
        # repo_list.append(repo_obj)
        # break
        repo_obj = Repository(repo_url, file_set)
        repo_list.append(repo_obj)
    print(f"Totally aggregate repo nums: {len(repo_list)}")

    return repo_list


if __name__ == "__main__":
    repo_list = aggregate()
    parsed_repo_list = list()
    for i, repo in enumerate(repo_list):
        print("=" * 36, ' ', repo.repo_url, ' ', "=" * 36)
        parsed_repo = parse_repo_files(repo)
        # print("hashid: ", parsed_repo.name2tab["escalations"].hashid)
        # print(parsed_repo.name2tab["escalations"].name2col)
        # print("hashid: ", parsed_repo.name2tab["items_tmp"].hashid)
        # print(parsed_repo.name2tab["items_tmp"].name2col)
        print(parsed_repo.name2tab)
        parsed_repo_list.append(parsed_repo)
    dump_repo_list(parsed_repo_list)
