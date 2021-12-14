# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import re
from dataclasses import dataclass


class RegexDict:
    """Define and retrieve regex by index"""
    __regex_dict = {
        "constraint_pk_create_table": "\((.*?)\)",
        "constraint_fk_create_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "constraint_unique_create_table": "\((.*?)\)",
        "startwith_fk_create_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "startwith_uk_create_table": "unique\s*key\s*.*?\((.*?)\)",
        "candidate_key_create_table": "\((.*?)\)",
        "startwith_ui_create_table": "unique\s+index\s+([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "add_constraint_pk_alter_table": "primary\s*key\s*\((.*?)\)",
        "add_pk_alter_table": "\((.*)\)",
        "add_constraint_fk_alter_table": "foreign\s*key\s*\(?(.*?)\)?\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "add_fk_alter_table": "\(([`|'|\"]?.*?[`|'|\"]?)\)\s*references\s*([`|'|\"]?.*?[`|'|\"]?)\s*\((.*?)\)",
        "add_unique_key_alter_table": "(.*?)\s*\((.*?)\)",
        "add_unique_index_alter_table": "\((.*?)\)",
        "add_constraint_unique_alter_table": "add\s*constraint\s*.*?\((.*?)\)",
    }

    @property
    def data(self):
        return self.__regex_dict

    def __getitem__(self, __key):
        if __key not in self.__regex_dict:
            raise KeyError("Please check input key for access related regex!")
        return self.__regex_dict[__key]

    def __call__(self, __key):
        return self.__getitem__(__key)


@dataclass
class Counter:
    """Class for nums counting.
    Return the number which have plus 1 after a function call.

    Params
    ------
    - None

    Returns
    -------
    - num: int
    """
    __num: int = 0

    @property
    def num(self):
        return self.__num

    def add(self):
        self.__num += 1
        return self.__num

    __call__ = add


def fmt_str(s):
    """Remove puncs like `, ', " for string which wrapped with them.

    Params
    ------
    - s: str

    Returns
    -------
    - str
    """
    return s.replace('\'', '').replace('"', '').replace('`', '').strip() if isinstance(s, str) else ""


def rm_kw(s):
    """Remove keywords like asc, desc after cols"""
    return s.replace(" asc", "").replace(" desc", "").strip()


def clean_stmt(stmt):
    """Remove useless keyword in SQL, e.g. COMMENT"""
    # remove COMMENT ...
    pattern = "(\s+comment\s*[\s|=]?\s*['|\"\`].*['|\"\`])[,|\n|;]"
    result = re.findall(pattern, stmt, re.IGNORECASE)
    if len(result) == 1:
        stmt = stmt.replace(result[0], "")
    return stmt


if __name__ == "__main__":
    # test for RegexDict
    regex_dict = RegexDict()
    print(regex_dict["add_constraint_pk_alter_table"])
    print(regex_dict("add_fk_alter_table"))
    print(regex_dict.data)

    # test for Counter
    counter = Counter()
    for i in range(100):
        print(counter())
