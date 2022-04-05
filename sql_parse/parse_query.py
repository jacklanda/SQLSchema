# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import os
import re
import glob
import logging
from random import sample
from pprint import pprint
from functools import lru_cache

from Levenshtein import distance
from sql_metadata import Parser
from sqlparse import parse
from sqlparse.sql import (
    Token,
    TokenList,
    Where,
    Function,
    Comparison,
    Identifier,
    Parenthesis,
    IdentifierList,
)

from utils import (
    Timeout,
    fmt_str,
    query_stmt_split,
    split_string,
    # convert_camel_to_underscore
)


class TableInstance:
    """Construct different table objects
    for the same table in the multiple-query statement.

    Params
    ------
    - table_name: str
    - table_obj: Table

    Attrib
    ------
    - table_name: str
    - table_obj: Table
    """

    def __init__(self, table_name, table_obj):
        self.__table_name = table_name
        self.__table_obj = table_obj

    @property
    def table_name(self):
        return self.__table_name

    @table_name.setter
    def table_name(self, name):
        self.__table_name = name

    @property
    def table_obj(self):
        return self.__table_obj

    @table_obj.setter
    def table_obj(self, obj):
        self.__table_obj = obj


class BinaryJoin:
    """Construct object and maintain infomation
    for binary join subclause in query statement.

    Params
    ------
    - table_a: TableInstance
    - table_b: TableInstance
    - conditions: list[tuple[Column, str, Column]]

    Attrib
    ------
    - table_a: TableInstance
    - table_b: TableInstance
    - conditions: list[tuple[Column, str, Column]]
      a list of binary operation tuples, each tuple
      includes three member: table_a's column, op and table_b's column.
    """

    def __init__(
        self,
        table_a,
        table_b,
    ):
        self.__table_a_obj = table_a
        self.__table_b_obj = table_b
        self.__condition_list = list()
        self.__join_type = "inner"

    @property
    def table_a(self):
        return self.__table_a_obj

    @table_a.setter
    def table_a(self, table_a):
        self.__table_a_obj = table_a

    @property
    def table_b(self):
        return self.__table_b_obj

    @table_b.setter
    def table_b(self, table_b):
        self.__table_b_obj = table_b

    @property
    def conditions(self):
        return self.__condition_list

    @conditions.setter
    def conditions(self, l):
        self.__condition_list = l

    @property
    def join_type(self):
        return self.__join_type

    @join_type.setter
    def join_type(self, join_type):
        self.__join_type = join_type


class Query:
    """Construct Query object to store a list of
    BinaryJoin objects in SQL query statements.

    Params
    ------
    - binary_join_list: list[BinaryJoin]

    Attribs
    -------
    - binary_joins: list[BinaryJoin]
    """

    def __init__(self, binary_join_list):
        self.__binary_join_list = binary_join_list

    def __len__(self):
        return len(self.__binary_join_list)

    @property
    def binary_joins(self):
        return self.__binary_join_list

    @binary_joins.setter
    def binary_joins(self, l):
        if not isinstance(l, list):
            raise TypeError("Could only assign attrib `binary_joins` with a list object!")
        self.__binary_join_list = l


class QueryNode:
    """Query Node class"""

    def __init__(self, token, parent=None):
        self.__token = token
        self.__parent = parent
        self.__children = list()
        self.__statement = self._normalize(token.value)
        self.__tables = list()
        self.__alias2table = dict()
        self.__limit_cols = list()
        self.__sub_query_list = list()
        self.__join_type = "inner"

    def _normalize(self, s):
        return s[1:-1].strip() if s[0] == '(' and s[-1] == ')' else s.strip()

    @ property
    def token(self):
        return self.__token

    @ property
    def parent(self):
        return self.__parent

    @ parent.setter
    def parent(self, p):
        self.__parent = p

    @ property
    def children(self):
        return self.__children

    @ property
    def statement(self):
        return self.__statement

    @ property
    def tables(self):
        return self.__tables

    @ tables.setter
    def tables(self, t):
        self.__tables = t

    @ property
    def alias2table(self):
        return self.__alias2table

    @ alias2table.setter
    def alias2table(self, d):
        self.__alias2table = d

    @ property
    def limit_cols(self):
        return self.__limit_cols

    @ limit_cols.setter
    def limit_cols(self, l):
        self.__limit_cols = l

    @property
    def sub_query_list(self):
        return self.__sub_query_list

    @sub_query_list.setter
    def sub_query_list(self, l):
        self.__sub_query_list = l

    @property
    def join_type(self):
        return self.__join_type

    @join_type.setter
    def join_type(self, join_type):
        self.__join_type = join_type


class QueryTree:
    """Query Tree class"""

    def __init__(self, root):
        self.__root = root

    @ property
    def root(self):
        return self.__root


class TokenVisitor:
    """TokenVisitor for visiting tokens in ast."""

    def __init__(self, root):
        self.root = self.curr = root
        self.tokens = self.get_all_tokens()
        self.tables = self.get_tables(root)
        self.select_tokens = self.get_all_select_tokens()

    def get_all_tokens(self):
        return list(self.root.flatten())

    def get_sub_token_list(self):
        return [t for t in self.curr.get_sublists()]

    def get_parent(self, token):
        return self.curr.parent

    def visit(self, token):
        # method = "visit_" + type(token).__name__
        method = type(token).__name__
        visitor = getattr(self, method, self.generic_visit)
        print(method)
        # print(visitor)
        return visitor(token)

    def generic_visit(self, token):
        if not isinstance(token, TokenList):
            return
        for tk in token:
            self.visit(tk)

    def get_all_select_tokens(self):

        def __is_join_query_stmt(t):
            # return True if t.value.lower() == "select" and str(t.ttype) == "Token.Keyword.DML" \
            return True if t.parent is not None and "select" in t.parent.value.lower() \
                and ("join" in t.parent.value.lower() or "where" in t.parent.value.lower()) else False

        # append SELECT clause followed by `INTERSECT`
        token_select = list()
        tokens = self.get_all_tokens()
        token_select = [t.parent for t in tokens if t.value.lower() == "select" and str(t.ttype) == "Token.Keyword.DML"]
        """
        token_select = [t.parent for t in tokens if t.value.lower() == "select" and str(t.ttype) == "Token.Keyword.DML"
                        and t.parent is not None and ("join" in t.parent.value.lower() or "where" in t.parent.value.lower())]
        """
        # token_select = [t.parent for t in tokens if __is_join_query_stmt(t)]
        if not token_select:
            token_select = list(set([t.parent.parent for t in tokens if t.value.lower() == "select"
                                     and str(t.ttype) == "Token.Keyword.DML" and __is_join_query_stmt(t.parent)]))

        return token_select

    def get_tables(self, token):
        res = dict()
        for d in self._get_tables(token):
            if fmt_str(d["alias"]) != "":
                res[fmt_str(d["alias"])] = fmt_str(d["table"])
        self._get_tables_patch(token, res)
        return res

    def _get_tables(self, token, i=None):
        i = token if i is None else i
        flag = False
        for i in getattr(i, "tokens", []):
            if isinstance(i, Token) and i.value.lower() == "from" or "join" in i.value.lower():
                flag = True
            elif isinstance(i, (Identifier, IdentifierList)) and flag:
                flag = False
                if not any(isinstance(x, Parenthesis) or 'select' in x.value.lower() for x in getattr(i, 'tokens', [])):
                    fr = ''.join(str(j) for j in i if j.value.lower() not in {'as', '\n'})
                    for t in re.findall('(?:\w+\.\w+|\w+)\s+\w+|(?:\w+\.\w+|\w+)', fr, re.IGNORECASE):
                        yield {'table': (t1 := t.split())[0], 'alias': None if len(t1) < 2 else t1[-1]}
            yield from self._get_tables(i)

    def _get_tables_patch(self, token, res):
        tokens = token.tokens
        for t in tokens:
            if isinstance(t, Identifier):
                if t.has_alias():
                    alias = fmt_str(t.get_alias())
                    tname = fmt_str(t.get_real_name())
                    res[alias] = tname
            # """
            elif isinstance(t, IdentifierList):
                if ',' in t.value:
                    table_pair_list = [t.strip() for t in t.value.split(',')]
                    for pair in table_pair_list:
                        if " as " in pair.lower():
                            t_name, t_alias = pair.lower().split(" as ", 1)
                            if t_alias not in res:
                                res[t_alias] = t_name
                        elif ' ' in pair:
                            t_name, t_alias = pair.split(' ', 1)
                            if t_alias not in res:
                                res[t_alias] = t_name
                        else:
                            if pair not in res:
                                res[pair] = pair
            # """
            elif "tokens" in dir(t):
                self._get_tables_patch(t, res)


class QueryParser:
    """QueryParser for a complete SQL query statement.
    1) generate ast using sqlparse
    2) traverse ast (maybe multiple times) and extract tables/column names
    3) according extracted entities construct BinaryJoin object
    4) construct Query object using BinaryJoin object list
    """

    def __init__(self, name2tab, is_debug=False):
        self.is_debug = is_debug
        self.alias2table = dict()
        self.alias2table_level = list()
        self.limit_cols_level = list()
        self.binary_join_list = list()
        self.condition_list = list()
        self.raw_condition_list = list()
        self.name2tab = name2tab
        # self.user_name2tab = user_name2tab
        # self.lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()} | {k.lower().rsplit('.', 1)[-1]: (k, v) for k, v in self.name2tab.items() if '.' in k}
        # self.lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()}
        self.only_two_join_tables = False
        self.single_query = False
        self.check_failed_cases = list()  # [(failed_condition, statement, dictionary)]
        self.unfound_tables = list()

    def _remove_duplicate_condition(self):
        self.binary_join_list = list(set(self.binary_join_list))

    def _has_sub_query(self, token):
        return token.is_keyword and token.value.lower() == "select"

    def _get_all_sub_query(self, select_tokens):
        p_tokens = [t.parent.value for t in select_tokens if t.parent is not None]
        # pp_tokens = [t.parent.value for t in p_tokens if t.parent is not None]
        # ppp_tokens = [t.parent.value for t in pp_tokens if t.parent is not None]
        select_clauses = list()
        all_clauses = p_tokens
        for c in all_clauses:
            if c[0] == '(' and c[-1] == ')':
                select_clauses.append(c[1:-1])
            else:
                select_clauses.append(c)
        return select_clauses

    def _get_likely_strs(self, s, m):
        # d = 999
        # likely_strs = list()
        d_str_pairs = list()
        for t_name, _ in m.items():
            d_str_pair = (distance(s, t_name), t_name)
            d_str_pairs.append(d_str_pair)
        sorted_d = sorted(d_str_pairs, key=lambda t: t[0])
        # return likely_str
        return sorted_d

    def _check_table_definition(self, table_name, col_name):

        def __normalize(s):
            l = list()
            l.append(s)
            if all(i in s for i in ('[', ']')):
                if '.' in s:
                    l.append(s.rsplit('.', 1)[1])
                s_without_square = s.replace('[', '').replace(']', '')
                l.append(s_without_square)
                l.append("#" + s_without_square)
                l.append("@" + s_without_square)
                l.append("public." + s_without_square)
                l.append("#public." + s_without_square)
                l.append("mydb." + s_without_square)
                l.append("#" + s)
                l.append("@" + s)
                l.append("[dbo]." + s)
                l.append("#[dbo]." + s)
            elif '.' in s:
                s_square = "".join(['[' + i + ']' for i in s.split('.')])
                l.append(s.split('.', 1)[1])
                l.append(s_square)
                l.append("[dbo]." + s_square)
                l.append("#[dbo]." + s_square)
                l.append("public." + s)
                l.append("#public." + s)
                l.append("#" + s)
                l.append("@" + s)
                l.append("dbo." + s)
                l.append("mydb." + s)
            else:
                s_square = '[' + s + ']'
                l.append(s_square)
                l.append("[dbo]." + s_square)
                l.append("#[dbo]." + s_square)
                l.append("public." + s)
                l.append("#public." + s)
                l.append("#" + s)
                l.append("@" + s)
                l.append("dbo." + s)
                l.append("mydb." + s)

            return l

        def __get_map(option):
            d = dict()
            m = self.name2tab
            if option == "name2tab":
                m = self.name2tab
            elif option == "user_name2tab":
                # m = self.user_name2tab
                pass
            for name, tab_obj in m.items():
                if '.' in name:
                    last_token = name.rsplit('.', 1)[1]
                    d[last_token.lower()] = (name, tab_obj)
                d[name.lower()] = (name, tab_obj)
            return d

        def __has_column(tab_obj, col_name):
            lower2name2col = {k.lower(): (k, v) for k, v in tab_obj.name2col.items()}
            return True if col_name.lower() in lower2name2col else False

        possible_items = __normalize(table_name) if '.' not in table_name else __normalize(table_name) + __normalize(table_name.rsplit('.', 1)[1])
        if self.name2tab:
            last_token2name_tab = __get_map("name2tab")
            # last_token2name_tab_user = __get_map("user_name2tab")
            # likely_strs = self._get_likely_strs(table_name, self.name2tab)
            # print(f"table_name: {table_name}'s most likely match result: {likely_strs}")
            for item in possible_items:
                # if item in self.name2tab:
                item = item.lower()
                if item in last_token2name_tab:
                    # only for production env
                    # tab_obj = last_token2name_tab[item][1]
                    # if __has_column(tab_obj, col_name):
                    return (True, last_token2name_tab[item][0])
                if item in self.name2tab:
                    # only for production env
                    # tab_obj = self.name2tab[item]
                    # if __has_column(tab_obj, col_name):
                    return (True, item)
                """
                if item in last_token2name_tab_user:
                    # only for production
                    # tab_obj = last_token2name_tab[item][1]
                    # if __has_column(tab_obj, col_name):
                    return (True, last_token2name_tab_user[item][0])
                """
        else:
            # print("empty name2tab")
            pass
        return (False, table_name)

    def _check_column_definition(self, table_obj, column_name):

        def __normalize(s):
            l = list()
            l.append(s)
            if all(i in s for i in ('[', ']')):
                s_without_square = s.replace('[', '').replace(']', '')
                l.append(s_without_square)
            else:
                s_with_square = '[' + s + ']'
                l.append(s_with_square)
            return l

        possible_items = __normalize(column_name.lower())
        lower2name2col = {k.lower(): (k, v) for k, v in table_obj.name2col.items()}
        if lower2name2col:
            # likely_strs = self._get_likely_strs(column_name, table_obj.name2col)
            # print(f"column_name: {column_name}'s most likely match result: {likely_strs}")
            for item in possible_items:
                if item in lower2name2col:
                    return (True, lower2name2col[item][0])
        return (False, column_name)

    def _insert_missing_table(self, tname):
        from cls_def import Table
        table_obj = Table(tname, "0")
        self.name2tab[tname] = table_obj
        return table_obj

    def _insert_missing_column(self, c_name, t_obj):
        from cls_def import Column
        c_obj = Column(c_name)
        t_obj.name2col[c_name] = c_obj
        return c_obj

    def _get_table_column_obj(self, column_name):
        for t_name, t_obj in self.name2tab.items():
            lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
            if column_name.lower() in lower2name2col:
                c_obj = lower2name2col[column_name.lower()][1]
                return t_obj, c_obj
        """
        for t_name, t_obj in self.user_name2tab.items():
            lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
            if column_name.lower() in lower2name2col:
                c_obj = lower2name2col[column_name.lower()][1]
                return t_obj, c_obj
        """

        tables = self.node.tables
        if tables is None or not tables:
            return None, None
        for t_name in tables:
            lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()}
            if t_name.lower() in lower2name2tab:
                t_obj = lower2name2tab[t_name.lower()][1]
                lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                if column_name.lower() in lower2name2col:
                    c_obj = lower2name2col[column_name.lower()][1]
                    return t_obj, c_obj

        return None, None

    def _construct_binaryjoin_object(self, l_tab_obj, l_col_obj, r_tab_obj, r_col_obj, op):
        """Construct a BinaryJoin object."""
        l_tab_instance = TableInstance(l_tab_obj.tab_name, l_tab_obj)
        r_tab_instance = TableInstance(r_tab_obj.tab_name, r_tab_obj)
        binaryjoin_obj = BinaryJoin(l_tab_instance, r_tab_instance)
        binaryjoin_obj.join_type = self.node.join_type
        binaryjoin_obj.conditions.append((l_col_obj, op, r_col_obj))
        return binaryjoin_obj

    def _construct_query_object(self):
        """Construct a query object according BinaryJoin object list."""
        return Query(self.binary_join_list) if self.binary_join_list else None

    def _get_binaryjoin_list(self, condition_list):

        def __get_entity(condition):
            left, op, right = condition
            l_tab, l_col = left.rsplit('.', 1)
            r_tab, r_col = right.rsplit('.', 1)
            return l_tab, l_col, r_tab, r_col, op

        binaryjoin_list = list()
        name_pair2obj_pair = dict()

        if self.only_two_join_tables:
            for condition in condition_list:
                left, op, right = condition
                if left.isdigit() or right.isdigit():
                    continue
                # print(f"input condition: {left} {op} {right}")
                if '.' in left:
                    l_tab, l_col = left.rsplit('.', 1)

                    is_exist, l_tab = self._check_table_definition(l_tab, l_col)
                    # l_tab_obj = self.name2tab[l_tab] if is_exist else self._insert_missing_table(l_tab)
                    # is_exist = True
                    if is_exist:
                        try:
                            l_tab_obj = self.name2tab[l_tab]
                        except:
                            # l_tab_obj = self.user_name2tab[l_tab]
                            continue
                        is_exist, l_col = self._check_column_definition(l_tab_obj, l_col)
                        if is_exist:
                            l_col_obj = l_tab_obj.name2col[l_col]
                        else:
                            # l_col_obj = self._insert_missing_column(l_col, l_tab_obj)
                            # print(f"column check fail: {l_col} in {left} {op} {right}")
                            self.check_failed_cases.append(((left, op, right), "failed on check column(left)", self.node.statement, l_tab_obj.name2col))
                            continue
                        try:
                            self.node.tables = self.node.tables.remove(l_tab_obj.tab_name) \
                                if self.node and self.node.tables and l_tab_obj.tab_name in self.node.tables else self.node.tables
                        except:
                            pass
                    else:
                        self.unfound_tables.append(l_tab)  # record unfound tables
                        # print(f"table check fail: {l_tab} in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check table(left)", self.node.statement, self.name2tab))
                        continue
                else:
                    if left.isdigit():
                        continue
                    l_tab_obj, l_col_obj = self._get_table_column_obj(left)
                    if l_tab_obj is None:
                        # print(f"table check fail: NotKnownTable in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check table(left)", self.node.statement, self.name2tab))
                        continue
                    if l_col_obj is None:
                        # print(f"column check fail: {left} in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check column(left)", self.node.statement, l_tab_obj.name2col))
                        continue
                    else:
                        l_tab = l_tab_obj.tab_name
                        l_col = left
                if '.' in right:
                    r_tab, r_col = right.rsplit('.', 1)

                    is_exist, r_tab = self._check_table_definition(r_tab, r_col)
                    # r_tab_obj = self.name2tab[r_tab] if is_exist else self._insert_missing_table(r_tab)
                    # is_exist = True
                    if is_exist:
                        try:
                            r_tab_obj = self.name2tab[r_tab]
                        except:
                            # r_tab_obj = self.user_name2tab[r_tab]
                            continue
                        is_exist, r_col = self._check_column_definition(r_tab_obj, r_col)
                        if is_exist:
                            r_col_obj = r_tab_obj.name2col[r_col]
                        else:
                            # r_col_obj = self._insert_missing_column(r_col, r_tab_obj)
                            # print(f"column check fail: {r_col} in {left} {op} {right}")
                            self.check_failed_cases.append(((left, op, right), "failed on check column(right)", self.node.statement, r_tab_obj.name2col))
                            continue
                        self.node.tables = self.node.tables.remove(r_tab_obj.tab_name) \
                            if self.node and self.node.tables and r_tab_obj.tab_name in self.node.tables else self.node.tables
                    else:
                        self.unfound_tables.append(r_tab)  # record unfound tables
                        # print(f"table check fail: {r_tab} in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check table(right)", self.node.statement, self.name2tab))
                        continue
                else:
                    if right.isdigit():
                        continue
                    r_tab_obj, r_col_obj = self._get_table_column_obj(right)
                    if r_tab_obj is None:
                        # print(f"table check fail: NotKnownTable in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check table(right)", self.node.statement, self.name2tab))
                        continue
                    if r_col_obj is None:
                        # print(f"column check fail: {right} in {left} {op} {right}")
                        self.check_failed_cases.append(((left, op, right), "failed on check column(right)", self.node.statement, r_tab_obj.name2col))
                        continue
                    else:
                        r_tab = r_tab_obj.tab_name
                        r_col = right

                if (l_tab, r_tab) in name_pair2obj_pair:
                    binaryjoin_obj = name_pair2obj_pair[(l_tab, r_tab)]
                    binaryjoin_obj.conditions.append((l_col_obj, op, r_col_obj))
                elif (r_tab, l_tab) in name_pair2obj_pair:
                    binaryjoin_obj = name_pair2obj_pair[(r_tab, l_tab)]
                    binaryjoin_obj.conditions.append((r_col_obj, op, l_col_obj))
                else:
                    binaryjoin_obj = self._construct_binaryjoin_object(l_tab_obj, l_col_obj, r_tab_obj, r_col_obj, op)
                    binaryjoin_list.append(binaryjoin_obj)
                    name_pair2obj_pair[(l_tab, r_tab)] = binaryjoin_obj
                # print(f"table and column check succ: {l_tab}.{l_col} {op} {r_tab}.{r_col}")
        # """
        else:
            for condition in condition_list:
                try:
                    l_tab, l_col, r_tab, r_col, op = __get_entity(condition)
                except:
                    continue
                if l_tab.isdigit() or l_col.isdigit() or r_tab.isdigit() or r_col.isdigit():
                    continue
                # print(f"input condition: {l_tab}.{l_col} {op} {r_tab}.{r_col}")

                is_exist, l_tab = self._check_table_definition(l_tab, l_col)
                # l_tab_obj = self.name2tab[l_tab] if is_exist else self._insert_missing_table(l_tab)
                # is_exist = True
                if is_exist:
                    try:
                        l_tab_obj = self.name2tab[l_tab]
                    except:
                        # l_tab_obj = self.user_name2tab[l_tab]
                        continue
                    is_exist, l_col = self._check_column_definition(l_tab_obj, l_col)
                    if is_exist:
                        l_col_obj = l_tab_obj.name2col[l_col]
                    else:
                        # l_col_obj = self._insert_missing_column(l_col, l_tab_obj)
                        # print(f"column check fail: {l_col} in {l_tab}.{l_col} {op} {r_tab}.{r_col}")
                        self.check_failed_cases.append((condition, "failed on check column(left)", self.node.statement, l_tab_obj.name2col))
                        continue
                else:
                    self.unfound_tables.append(l_tab)  # record unfound tables
                    # print(f"table check fail: {l_tab} in {l_tab}.{l_col} {op} {r_tab}.{r_col}")
                    self.check_failed_cases.append((condition, "failed on check table(left)", self.node.statement, self.name2tab))
                    continue

                is_exist, r_tab = self._check_table_definition(r_tab, r_col)
                # r_tab_obj = self.name2tab[r_tab] if is_exist else self._insert_missing_table(r_tab)
                # is_exist = True
                if is_exist:
                    try:
                        r_tab_obj = self.name2tab[r_tab]
                    except:
                        # r_tab_obj = self.user_name2tab[r_tab]
                        continue
                    is_exist, r_col = self._check_column_definition(r_tab_obj, r_col)
                    if is_exist:
                        r_col_obj = r_tab_obj.name2col[r_col]
                    else:
                        # r_col_obj = self._insert_missing_column(r_col, r_tab_obj)
                        # print(f"column check fail: {r_col} in {l_tab}.{l_col} {op} {r_tab}.{r_col}")
                        self.check_failed_cases.append((condition, "failed on check column(right)", self.node.statement, r_tab_obj.name2col))
                        continue
                else:
                    self.unfound_tables.append(r_tab)  # record unfound tables
                    # print(f"table check fail: {r_tab} in {l_tab}.{l_col} {op} {r_tab}.{r_col}")
                    self.check_failed_cases.append((condition, "failed on check table(right)", self.node.statement, self.name2tab))
                    continue

                if (l_tab, r_tab) in name_pair2obj_pair:
                    binaryjoin_obj = name_pair2obj_pair[(l_tab, r_tab)]
                    binaryjoin_obj.conditions.append((l_col_obj, op, r_col_obj))
                elif (r_tab, l_tab) in name_pair2obj_pair:
                    binaryjoin_obj = name_pair2obj_pair[(r_tab, l_tab)]
                    binaryjoin_obj.conditions.append((r_col_obj, op, l_col_obj))
                else:
                    binaryjoin_obj = self._construct_binaryjoin_object(l_tab_obj, l_col_obj, r_tab_obj, r_col_obj, op)
                    binaryjoin_list.append(binaryjoin_obj)
                    name_pair2obj_pair[(l_tab, r_tab)] = binaryjoin_obj
                # print(f"table and column check succ: {l_tab}.{l_col} {op} {r_tab}.{r_col}")
        # """

        return binaryjoin_list

    def _get_limit_cols(self, metadata):
        limit_cols_join = metadata.columns_dict["join"] \
            if metadata.columns_dict.get("join") is not None else list()
        limit_cols_where = metadata.columns_dict["where"] \
            if metadata.columns_dict.get("where") is not None else list()
        return limit_cols_join + limit_cols_where

    def _find_table_in_children(self, alias, column):

        def __is_in_columns(column):
            for c in metadata.columns:
                if '.' in c:
                    tab_name, col_name = c.rsplit(".", 1)
                    if column == col_name:
                        return (True, tab_name)
            return (False, alias)

        alias2query = dict()
        for d in self.node.sub_query_list:
            if d is not None:
                alias2query |= d
                alias2query |= {k.lower(): v for k, v in d.items()}
        sub_query = alias2query[alias]
        metadata = Parser(sub_query)
        lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()}
        if metadata.columns_aliases and column in metadata.columns_aliases:
            column = metadata.columns_aliases[column]
        if metadata.tables:
            for table_name in metadata.tables:
                if table_name.lower() in lower2name2tab:
                    t_obj = lower2name2tab[table_name.lower()][1]
                    lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                    if isinstance(column, str) and column.lower() in lower2name2col:
                        return t_obj.tab_name, lower2name2col[column.lower()][0]
                    else:
                        for c in column:
                            if c.lower() in lower2name2col:
                                return t_obj.tab_name, lower2name2col[c.lower()][0]
        if len(metadata.tables) == 1:
            return (metadata.tables[0], column)
        if metadata.columns:
            is_find_table, table_name = __is_in_columns(column)
            if is_find_table:
                return table_name, column
        if isinstance(column, str) and column in metadata.columns_aliases:
            column = metadata.columns_aliases[column].rsplit('.', 1)[-1]
            col2tab = {p.rsplit('.', 1)[-1]: p.rsplit('.', 1)[-2] for p in metadata.columns_dict["select"] if '.' in p}
            return (col2tab[column], column) if column in col2tab else (alias, column)
        return alias, column

    def _find_table_in_parent(self, alias_or_name, parent):
        a2t = parent.alias2table
        return (a2t[alias_or_name], True) if alias_or_name in a2t else (alias_or_name, False)

    def _get_left_right(self, condition, op):

        def __has_matched_subquery(alias):
            alias_list = list()
            for d in self.node.sub_query_list:
                if d is not None:
                    alias_list += d.keys()
                    alias_list += [s.lower() for s in d.keys()]
            return True if alias in alias_list else False

        def __rm_double_colon(s):
            return s.rsplit("::", 1)[0].strip() if "::" in s else s

        limit_cols = self.node.limit_cols
        alias2table = self.node.alias2table
        left = fmt_str(__rm_double_colon(condition.split(op, 1)[0].strip()).lower())
        right = fmt_str(__rm_double_colon(condition.split(op, 1)[1].strip()).lower())

        if '.' in left:
            # find table's alias in current scope
            left_table, left_column = left.rsplit('.', 1)
            if left_table in alias2table:
                left_table = alias2table[left_table]
            # find table's alias in children scope
            elif __has_matched_subquery(left_table):
                left_table, left_column = self._find_table_in_children(left_table, left_column)
            # find table's alias in parent scope
            else:
                parent = self.node.parent
                while parent is not None:
                    left_table, found = self._find_table_in_parent(left_table, parent)
                    if found:
                        break
                    parent = parent.parent
                col_name = left_table.strip() + '.' + left_column.strip()
                limit_cols.append(col_name)
            if not isinstance(left_table, str):
                left_table = left_table[0]
            if not isinstance(left_column, str):
                left_column = left_column[0]
            left = left_table.strip() + '.' + left_column.strip()
            if left not in self.node.limit_cols:
                self.node.limit_cols.append(left)
        else:
            # elif self.only_two_join_tables and self.node.tables:
            left_old = left
            for t_name, t_obj in self.name2tab.items():
                lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                if left.lower() in lower2name2col:
                    left = t_obj.tab_name + '.' + left.strip()
                    break
            # iterate in user_name2tab
            """
            if left == left_old:
                for t_name, t_obj in self.user_name2tab.items():
                    lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                    if left.lower() in lower2name2col:
                        left = t_obj.tab_name + '.' + left.strip()
                        break
            """
            left = left if left != left_old else None
            """
            lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()} | {k.lower().rsplit('.', 1)[-1]: (k, v) for k, v in self.name2tab.items() if '.' in k}
            for tname in self.node.tables:
                if tname.lower() in lower2name2tab:
                    table_obj = lower2name2tab[tname.lower()][1]
                    lower2name2col = {k.lower(): (k, v) for k, v in table_obj.name2col.items()}
                    if left.lower() in lower2name2col:
                        left = table_obj.tab_name + '.' + left.strip()
                        break
            """
            if left is not None and left not in self.node.limit_cols:
                self.node.limit_cols.append(left)

        if '.' in right:
            right_table, right_column = right.rsplit('.', 1)
            # find table's alias in current scope
            if right_table in alias2table:
                right_table = alias2table[right_table]
            # find table's alias in children scope
            elif __has_matched_subquery(right_table):
                right_table, right_column = self._find_table_in_children(right_table, right_column)
            # find table's alias in parent scope
            else:
                parent = self.node.parent
                while parent is not None:
                    right_table, found = self._find_table_in_parent(right_table, parent)
                    if found:
                        break
                    parent = parent.parent
                col_name = right_table.strip() + '.' + right_column.strip()
                limit_cols.append(col_name)
            if not isinstance(right_table, str):
                right_table = right_table[0]
            if not isinstance(right_column, str):
                right_column = right_column[0]
            right = right_table.strip() + '.' + right_column.strip()
            if right not in self.node.limit_cols:
                self.node.limit_cols.append(right)
        else:
            right_old = right
            for t_name, t_obj in self.name2tab.items():
                lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                if right.lower() in lower2name2col:
                    right = t_obj.tab_name + '.' + right.strip()
                    break
            # iterate in user_name2tab
            """
            if right == right_old:
                for t_name, t_obj in self.user_name2tab.items():
                    lower2name2col = {k.lower(): (k, v) for k, v in t_obj.name2col.items()}
                    if right.lower() in lower2name2col:
                        right = t_obj.tab_name + '.' + right.strip()
                        break
            """
            right = right if right != right_old else None
            """
            lower2name2tab = {k.lower(): (k, v) for k, v in self.name2tab.items()} | {k.lower().rsplit('.', 1)[-1]: (k, v) for k, v in self.name2tab.items() if '.' in k}
            for tname in self.node.tables:
                if tname.lower() in lower2name2tab:
                    table_obj = lower2name2tab[tname.lower()][1]
                    lower2name2col = {k.lower(): (k, v) for k, v in table_obj.name2col.items()}
                    if right.lower() in lower2name2col:
                        right = table_obj.tab_name + '.' + right.strip()
                        break
            if right not in self.node.limit_cols:
                self.node.limit_cols.append(right)
            else:
                right = None
            """
            if right is not None and right not in self.node.limit_cols:
                self.node.limit_cols.append(right)
        return left, right

    def _get_mutual_map(self, alias2table):
        name2name = {name: name for alias, name in alias2table.items()}
        return name2name | alias2table

    def _get_alias2table(self, s):
        a2t = dict()
        # clause = s[s.index("from"):s.index("where")].replace("FROM", "").replace("from", "").strip()
        clause = split_string(split_string(s, "from"), "where", get_first=True).replace("FROM", "").replace("from", "").strip()
        # print(clause)
        items = [i.strip() for i in clause.split(',')]
        for item in items:
            if " as " in item.lower():
                # name, alias = item.split(" as ")
                name, alias = re.split(" as | AS | As | aS ", item)
                a2t[alias.strip()] = name.strip()
            elif " " in item:
                name, alias = item.rsplit(" ", 1)
                a2t[alias.strip()] = name.strip()
            else:
                name, alias = item, None
                a2t[name.strip()] = name.strip()
        return a2t

    def _normalize_condition(self, condition_list, metadata):

        def __get_lower_alias2table(m):
            return m | {k.lower(): v for k, v in m.items()} | {k.rsplit('.', 1)[-1]: v for k, v in m.items() if '.' in k} | {k.rsplit('.', 1)[-1].lower(): v for k, v in m.items() if '.' in k}

        def __rm_substr_after_last_space(s):
            return s.rsplit(' ', 1)[0].strip() if '[' not in s and ']' not in s and ' ' in s else s

        alias2table = dict()
        normal_conditions = list()

        try:
            # alias2table = {k: v for k, v in self.visitor.get_tables(self.node.token).items() if k is not None and k != ""}
            alias2table = self._get_mutual_map(metadata.tables_aliases)
        except ValueError:
            alias2table = self._get_mutual_map(metadata.tables_aliases)
        except Exception:
            alias2table = {k: v for k, v in self.visitor.get_tables(self.node.token).items() if k is not None and k != ""}
            alias2table = self._get_mutual_map(alias2table)
        finally:
            """
            alias2table_patch = {k: v for k, v in self.visitor.get_tables(self.node.token).items() if k is not None and k != ""}
            for k, v in alias2table_patch.items():
                if k not in alias2table:
                    alias2table[k] = v
            """
            alias2table_patch = {k: v for k, v in self.visitor.tables.items() if k is not None and k != ""}
            for k, v in alias2table_patch.items():
                if k not in alias2table:
                    alias2table[k] = v
            self.node.alias2table = alias2table

        if self.single_query and not alias2table:
            try:
                alias2table = self._get_alias2table(self.node.statement)
                self.node.alias2table = alias2table
            except:
                pass

        if not alias2table:
            alias2table = {k: v for k, v in self.visitor.get_tables(self.node.token).items() if k is not None and k != ""}
            alias2table = self._get_mutual_map(alias2table)
            self.node.alias2table = alias2table

        self.node.alias2table = __get_lower_alias2table(self.node.alias2table)

        try:
            self.node.limit_cols = self._get_limit_cols(metadata)
        except:
            self.node.limit_cols = list()

        for condition in condition_list:
            if "@" in condition:
                continue
            if "!=" in condition:
                continue
            elif "<=" in condition:
                op = "LtEq"
                left, right = self._get_left_right(condition, "<=")
            elif ">=" in condition:
                op = "GtEq"
                left, right = self._get_left_right(condition, ">=")
            elif "<>" in condition:
                continue
            elif "<" in condition:
                op = "Lt"
                left, right = self._get_left_right(condition, "<")
            elif ">" in condition:
                op = "Gt"
                left, right = self._get_left_right(condition, ">")
            elif "=" in condition:
                op = "Eq"
                condition = condition.replace("==", "=")
                left, right = self._get_left_right(condition, "=")
            else:
                continue

            if left is None or right is None:
                continue
            if self.node.limit_cols:
                if left not in self.node.limit_cols or right not in self.node.limit_cols:
                    continue

            left, right = __rm_substr_after_last_space(left), __rm_substr_after_last_space(right)
            left, right = fmt_str(left), fmt_str(right)
            normal_conditions.append((left, op, right))

        return normal_conditions

    def _get_tokens(self, token=None, stmt=None):
        tokens_size = 1
        with Timeout(10):
            while tokens_size == 1:
                try:
                    tokens = parse(stmt)[0].tokens if stmt else token.tokens
                except Exception as e:
                    raise e
                else:
                    if len(tokens) == 1:
                        tokens = self._get_tokens(token=tokens[0])
                    tokens_size = len(tokens)
            return tokens

    def _exclude_clause(self, s):
        s_lower = s.lower()
        if ".\'" in s_lower or ".\"" in s_lower or "\'." in s_lower or "\"." in s_lower:
            return True if "select " not in s_lower\
                and "from " not in s_lower\
                and "join " not in s_lower\
                and "where " not in s_lower\
                and "," not in s_lower\
                and "(" not in s_lower\
                and ")" not in s_lower\
                and "+" not in s_lower\
                and "?" not in s_lower\
                and "@" not in s_lower\
                and ":" not in s_lower\
                else False
        else:
            return True if "select " not in s_lower\
                and "from " not in s_lower\
                and "join " not in s_lower\
                and "where " not in s_lower\
                and "\"" not in s_lower\
                and "\'" not in s_lower\
                and "," not in s_lower\
                and "(" not in s_lower\
                and ")" not in s_lower\
                and "+" not in s_lower\
                and "?" not in s_lower\
                and "@" not in s_lower\
                and ":" not in s_lower\
                else False

    def _extract_conditions(self, tokens):

        def __include_literal(t):
            for t in t.tokens:
                if str(t.ttype) == "Token.Literal.String.Single" or isinstance(t, Function):
                    return True
            return False

        def __extract_continuous_cmp_tokens(token):
            if not token.is_group:
                return
            token_list = token.tokens
            pos = 2
            while pos < len(token_list):
                if isinstance(token_list[pos], Comparison) \
                        and self._exclude_clause(token_list[pos].value) \
                        and not __include_literal(token_list[pos]):
                    condition_list.append(token_list[pos].value)
                elif isinstance(token_list[pos], Function):
                    __extract_internal_cmp_tokens(token)
                elif isinstance(token_list[pos], Parenthesis):
                    __extract_internal_cmp_tokens(token)
                pos += 1

        def __extract_internal_cmp_tokens(token):
            if not token.is_group:
                return
            intern_tokens = token.tokens
            for token in intern_tokens:
                if isinstance(token, Comparison) \
                        and self._exclude_clause(token.value) \
                        and not __include_literal(token):
                    condition_list.append(token.value)
                elif isinstance(token, Where):
                    __extract_continuous_cmp_tokens(token)
                elif isinstance(token, Parenthesis):
                    __extract_internal_cmp_tokens(token)
                elif isinstance(token, Function):
                    __extract_internal_cmp_tokens(token)

        condition_list = list()
        for token in tokens:
            if isinstance(token, Comparison) \
                    and self._exclude_clause(token.value) \
                    and not __include_literal(token):
                condition_list.append(token.value)
            elif isinstance(token, Where):
                __extract_continuous_cmp_tokens(token)
            elif isinstance(token, Function):
                condition_list += self._extract_conditions(token.tokens)
            elif isinstance(token, Parenthesis):
                # handle join condition in ()
                __extract_internal_cmp_tokens(token)

        return condition_list

    def filter_raw_conditions(self, condition_list):

        def __equal_to_any(s):
            return True if any([s == i for i in ["default", "true", "false", "null"]]) else False

        def __is_numeric(s):
            s = s.replace('$', '').strip()
            try:
                float(s)
                return True
            except ValueError:
                return False

        filter_conditions = list()
        for condition in condition_list:
            if "!=" in condition or "<>" in condition:
                continue
            if '=' in condition:
                condition = condition.replace("==", "=")
                left, right = condition.split('=', 1)
                left, right = left.strip(), right.strip()
            elif '>=' in condition:
                left, right = condition.split('>=', 1)
                left, right = left.strip(), right.strip()
            elif '<=' in condition:
                left, right = condition.split('<=', 1)
                left, right = left.strip(), right.strip()
            elif '>' in condition:
                left, right = condition.split('>', 1)
                left, right = left.strip(), right.strip()
            elif '<' in condition:
                left, right = condition.split('<', 1)
                left, right = left.strip(), right.strip()
            else:
                continue
            if __is_numeric(left) or __is_numeric(right) \
                    or __equal_to_any(left) or __equal_to_any(right) \
                    or left.startswith('@') or right.startswith('@') \
                    or left.count(' ') > 1 or right.count(' ') > 1:
                continue
            filter_conditions.append(condition)
        return filter_conditions

    def _parse_select_join_query(self, stmt):
        try:
            tokens = self._get_tokens(stmt=stmt)
        except Exception as e:
            tokens = parse(stmt)[0].tokens
            # raise e
        try:
            metadata = Parser(stmt)
        except:
            # stmt = stmt[stmt.index("select "):]
            stmt = split_string(stmt, "select ")
            metadata = Parser(stmt)
        try:
            self.only_two_join_tables = True \
                if len(metadata.tables) == 2 \
                or ("select into" in stmt.lower() and len(metadata.tables) == 3) else False
        except:
            pass
        else:
            try:
                self.node.tables = metadata.tables
            except Exception as e:
                self.node.tables = metadata.tables
            except:
                print("get tables from metadata error!")

        condition_list = self._extract_conditions(tokens)
        condition_list = self.filter_raw_conditions(condition_list)
        self.raw_condition_list += condition_list

        normal_conditions = self._normalize_condition(condition_list, metadata)
        normal_conditions = list(set(normal_conditions))
        self.condition_list += normal_conditions

        if not self.is_debug:
            binary_join_list = self._get_binaryjoin_list(normal_conditions)
            self.binary_join_list += binary_join_list

    def _parse_select_where_query(self, stmt):

        def __include_literal(t):
            for t in t.tokens:
                if str(t.ttype) == "Token.Literal.String.Single" or isinstance(t, Function):
                    return True
            return False

        def __find_all_where_tokens(tokens):
            return [t for t in tokens if isinstance(t, Where) if t.value.lower() != "select" and "where" in t.value.lower()]

        def __find_all_cmp_tokens(tokens):
            l = list()
            for token in tokens:
                if isinstance(token, Comparison) and token.value not in condition_list \
                        and not __include_literal(token) and self._exclude_clause(token.value):
                    l.append(fmt_str(token.value))
            return l

        def __get_condition_str(l):
            for where_token in l:
                # yield where_token.value.split("where")[1].strip()
                yield split_string(where_token.value, "where").strip()

        try:
            tokens = parse(stmt)[0].tokens
        except Exception as e:
            raise e
        try:
            metadata = Parser(stmt)
        except Exception as e:
            raise e

        condition_list = list()
        where_tokens = __find_all_where_tokens(tokens)

        for where_token in where_tokens:
            condition_list += __find_all_cmp_tokens(where_token.tokens)

        for condition_str in __get_condition_str(where_tokens):
            if " and " in condition_str.lower():
                condition_list += [c.strip() for c in re.split(" and | AND | And ", condition_str) if self._exclude_clause(c) and c.strip() not in condition_list]
            elif " or " in condition_str.lower() and self._exclude_clause(condition_str):
                condition_list += [c.strip() for c in re.split(" or | OR | Or | oR ", condition_str) if self._exclude_clause(c) and c.strip() not in condition_list]
            elif self._exclude_clause(condition_str) and condition_str.strip() not in condition_list:
                condition_list.append(condition_str.strip())

        condition_list = self.filter_raw_conditions(condition_list)
        self.raw_condition_list += condition_list

        normal_conditions = self._normalize_condition(condition_list, metadata)
        normal_conditions = list(set(normal_conditions))
        self.condition_list += normal_conditions

        if not self.is_debug:
            binary_join_list = self._get_binaryjoin_list(normal_conditions)
            self.binary_join_list += binary_join_list

    def _find_table_in_subquery(self, column_name, subquery):
        subquery_metadata = Parser(subquery)
        try:
            projections = subquery_metadata.columns_dict["select"]
        except:
            return
        for col in projections:
            if '.' in col:
                tab_name, col_name = col.rsplit('.', 1)
                if col_name == column_name:
                    return tab_name
        return

    def _parse_outter_join(self, metadata):

        def repl_op(condition):
            if "!=" in condition:
                return
            elif "<=" in condition:
                op = "LtEq"
                left, right = condition.split("<=", 1)
            elif ">=" in condition:
                op = "GtEq"
                left, right = condition.split(">=", 1)
            elif "=" in condition:
                op = "Eq"
                condition = condition.replace("==", "=")
                left, right = condition.split("=", 1)
            elif "<" in condition:
                op = "Lt"
                left, right = condition.split("<", 1)
            elif ">" in condition:
                op = "Gt"
                left, right = condition.split(">", 1)
            elif "<>" in condition:
                return
            else:
                return
            return left, op, right

        def _match_condition(condition, subquery, outter_alias2table):
            if condition is None:
                return
            left, op, right = condition
            subquery_alias = subquery.value.rsplit(' ', 1)[1]
            if '.' in left:
                left_table, left_column = left.rsplit('.', 1)
            else:
                return
            if '.' in right:
                right_table, right_column = right.rsplit('.', 1)
            else:
                return
            if left_table == subquery_alias:
                left_table = self._find_table_in_subquery(left_column, subquery.value)
                if right_table in outter_alias2table:
                    right_table = outter_alias2table[right_table]
            elif right_table == subquery_alias:
                right_table = self._find_table_in_subquery(right_column, subquery.value)
                if left_table in outter_alias2table:
                    left_table = outter_alias2table[left_table]
            else:
                return
            if left_table is None or right_table is None:
                return
            return left_table + '.' + left_column, op, right_table + '.' + right_column

        root = self.visitor.root
        outter_alias2table = {k: v for k, v in self.visitor.tables.items()}
        # print(outter_alias2table)
        outter_alias2table = self._get_mutual_map(outter_alias2table)
        condition_list = list()
        outter_join_list = list()
        root_sub_list = [t for t in root.get_sublists()]

        for pos, token in enumerate(root_sub_list):
            if pos + 1 < len(root_sub_list):
                next_token = root_sub_list[pos + 1]
            else:
                continue
            if isinstance(token, Identifier) and "select " in token.value.lower() \
                    and isinstance(next_token, Comparison):
                outter_join_list.append((token, next_token))
        # subqueries = metadata.subqueries
        for (subquery, condition) in outter_join_list:
            condition = repl_op(condition.value)
            res = _match_condition(condition, subquery, outter_alias2table)
            if res:
                condition_list.append(res)

        self.raw_condition_list += condition_list

        condition_list = list(set(condition_list))
        self.condition_list += condition_list

        if not self.is_debug:
            self.binary_join_list += self._get_binaryjoin_list(condition_list)

    def _find_internal_query(self, stmt):
        left_pos, right_pos = 0, len(stmt) - 1
        left_parenthesis_num, right_parenthesis_num = 0, 0
        is_end = False
        while left_pos < len(stmt) and right_pos >= 0:
            while left_pos < len(stmt):
                if left_pos + 6 < len(stmt) and stmt[left_pos:left_pos + 6].lower() == "select":
                    is_end = True
                    break
                if stmt[left_pos] == '(':
                    left_parenthesis_num += 1
                    break
                left_pos += 1
            if is_end:
                break
            while right_pos > 0:
                if stmt[right_pos] == ')':
                    right_parenthesis_num += 1
                    break
                right_pos -= 1
            left_pos += 1

        return stmt if left_parenthesis_num != right_parenthesis_num else stmt[left_pos:right_pos + 1]

    def _get_subqueries(self):

        def __find_alias_in_parent(token):
            if token.parent:
                return token.parent.get_alias() if token.parent.get_alias() else __find_alias_in_parent(token.parent)

        d = dict()
        if not self.node.children:
            return d
        for child in self.node.children:
            if child is None:
                continue
            alias = __find_alias_in_parent(child.token)
            if alias is not None:
                d[alias] = child.statement
        return d

    def _parse_single_query_statement(self, stmt):
        """Parse single select statement."""
        self.single_query = True
        if all(s in stmt.lower() for s in ("select", "join")):
            self._parse_select_join_query(stmt)
            # self._parse_select_where_query(stmt)
        elif all(s in stmt.lower() for s in ("select", "where")):
            self._parse_select_where_query(stmt)

    def _parse_multiple_query_statement(self, stmt):
        """Parse multiple select statement.
        Include: 1. union query, 2. nested query.
        """
        metadata = Parser(fmt_str(stmt))
        try:
            if metadata.subqueries:
                self.node.sub_query_list.append(metadata.subqueries)
        except:
            try:
                if metadata.subqueries:
                    self.node.sub_query_list.append(metadata.subqueries)
            except:
                pass
        # if not self.node.sub_query_list:
        self.node.sub_query_list.append(self._get_subqueries())
        # print(self.node.sub_query_list)

        # TODO: get outter alias2table from every subqueries.
        # if all(s in stmt.lower() for s in ("select ", "join ")):
        if all(s in stmt.lower() for s in ("select", "join")):
            self._parse_select_join_query(stmt)
            self._parse_select_where_query(stmt)
        # elif all(s in stmt.lower() for s in ("select ", "where ")):
        elif all(s in stmt.lower() for s in ("select", "where")):
            self._parse_select_where_query(stmt)

        # handle subquery joins
        self._parse_outter_join(metadata)

    def build_query_tree(self, token_nodes):

        def __get_join_type(query_node):
            stmt = query_node.statement.lower()
            for child in query_node.children:
                stmt = stmt.replace(child.statement.lower(), "")
            if "join" not in stmt:
                return "inner"
            else:
                join_num = list()
                join_num.append(("inner", stmt.count("inner join")))
                join_num.append(("left", stmt.count("left join") + stmt.count("left outer join")))
                join_num.append(("right", stmt.count("right join") + stmt.count("right outer join")))
                join_num.append(("full", stmt.count("full join") + stmt.count("full outer join")))
                join_num.append(("cross", stmt.count("cross join")))
                join_num.sort(key=lambda x: x[1], reverse=True)
            return join_num[0][0]

        def __get_subquery_list(query_node):
            subquery_list = list()
            children = query_node.children
            for child in children:
                token = child.token.parent
                if token.has_alias():
                    sub_list = [t for t in token.get_sublists()]
                    subquery = sub_list[0].value[1:-1]
                    alias = sub_list[1].value
                    subquery_list.append({alias: subquery})
            return subquery_list

        query_nodes = list()

        # links all nodes' parent
        for i in range(len(token_nodes)):
            node = QueryNode(token_nodes[i])
            query_nodes.append(node)
            for j in range(len(query_nodes)):
                if node.token.has_ancestor(query_nodes[j].token):
                    node.parent = query_nodes[j]

        # links all nodes' children
        for query_node in query_nodes:
            if query_node.parent:
                query_node.parent.children.append(query_node)

        # generate subquery list
        for query_node in query_nodes:
            try:
                subquery_list = __get_subquery_list(query_node)
                # print(query_node.sub_query_list)
            except:
                pass
            else:
                query_node.sub_query_list = subquery_list
                # print(query_node.sub_query_list)

        # get join type
        for query_node in query_nodes:
            query_node.join_type = __get_join_type(query_node)

        return query_nodes

    def print_query_tree(self, query_nodes):
        for query_node in query_nodes:
            # print(query_node, query_node.parent)
            print(query_node, query_node.children)
            print(query_node.statement)
            print()

    def parse_query_nodes(self, nodes):
        if len(nodes) == 1:
            stmt = nodes[0].statement
            self.node = nodes[0]
            try:
                self._parse_single_query_statement(stmt)
            except Exception as e:
                # print("Query parse error:", e)
                logging.exception(e)
            # try to extract SELECT part in stmt
            if not self.is_debug:
                if not self.binary_join_list:
                    try:
                        stmt = self._find_internal_query(stmt)
                        self._parse_single_query_statement(stmt)
                    except Exception as e:
                        # print("Query parse error:", e)
                        logging.exception(e)
        # handle multiple-select query
        elif len(nodes) > 1:
            # print(nodes[0].children[0].statement)
            for node in nodes:
                stmt = node.statement
                self.node = node
                # print(stmt)
                try:
                    self._parse_multiple_query_statement(stmt)
                except Exception as e:
                    # print("Query parse error:", e)
                    logging.exception(e)

    def _preprocess(self, s):
        return s.replace("(nolock)", "").replace("(NOLOCK)", "").replace("(+)", "").replace("(-)", "")

    @lru_cache(1024)
    def is_union_query(self, t, is_exist=False):
        if "tokens" in dir(t):
            tokens = t.tokens
            for token in tokens:
                if self.is_union_query(token):
                    return True
        elif t.is_keyword and (t.value.lower() == "union" or t.value.lower() == "union all"):
            return True
        return False

    def parse(self, s):
        s = self._preprocess(s)
        root, = parse(s)
        self.visitor = TokenVisitor(root)
        select_tokens = self.visitor.select_tokens

        # if self.is_union_query(root):
        # print("is union query:", s)
        # else:
        # print("not union query:", s)
        # return

        if not select_tokens:
            s = s[s.lower().index("select"):]
            root, = parse(s)
            self.visitor = TokenVisitor(root)
            select_tokens = self.visitor.select_tokens

        query_nodes = self.build_query_tree(select_tokens)
        # handle single-select query
        self.parse_query_nodes(query_nodes)

        if not self.is_debug:
            # if not self.binary_join_list and self.condition_list:
            if not self.binary_join_list and self.raw_condition_list:
                # print("input query statement")
                if self.condition_list:
                    # print("query parse succ and link fail, have normalized condition")
                    pass
                else:
                    pass
                    # print("query parse succ and link fail, have not normalized condition")
                    # print(s)
                    # print(self.raw_condition_list)
            # elif not self.binary_join_list and not self.condition_list:
            elif not self.binary_join_list and not self.raw_condition_list:
                pass
                # print("query parse fail and link fail")
                # print(s)
            else:
                pass
                # print("input query statement")
                # print("query parse succ and link succ")
                # print(self.binary_join_list)

        query_object = self._construct_query_object()

        # check_failed_cases = self.check_failed_cases

        # return query_object, self.unfound_tables
        return query_object


if __name__ == "__main__":
    query_list = list()
    stmts = [
        """insert into nodes(wc_id, local_relpath, op_depth, parent_relpath, presence, kind)
select wc_id, local_relpath, ? 4, parent_relpath, map_base_deleted, kind
from nodes
where
    wc_id = ? 1 and (
        local_relpath = ? 2 or is_strict_descendant_of(local_relpath, ? 2)
    ) and op_depth = ? 3 and presence not in (
        map_base_deleted, map_not_present, map_excluded, map_server_exclu ded
    ) and file_external is null"""
    ]

    for stmt in stmts:
        # stmt = stmt.lower()
        stmt = ' '.join(stmt.split())
        parser = QueryParser(dict(), is_debug=True)
        parser.parse(stmt)
        print(parser.condition_list)
        del parser

    exit()

    import time
    from pickle import load, dump
    from parse_join_query import SqlparseParser, print_join_obj
    from repo_parse_sql import Repository

    sample_num = 8700
    # fpath = f"data/samples/fpath_list_{str(sample_num)}_{time.strftime('%Y_%m_%d_%H:%M:%S')}.pkl"
    INPUT_FOLDER = os.path.join(os.getcwd(), "data/s3_sql_files_crawled_all_vms")
    files = [f for f in glob.glob(os.path.join(INPUT_FOLDER, "*.sql"))]
    fp_list = sample(files, sample_num)
    # fp_list = files
    # fpath = "data/samples/fpath_list_8700_2022_02_09_15:23:49.pkl"
    # fp_list = sample(load(open(fpath, "rb")), sample_num)
    # fp_list = ["failed_cases.txt"]
    # fp_list = load(open(fpath, "rb"))
    # fp_list = files
    succ, fail = 0, 0
    query_stmt_list = list()
    for fp in fp_list:
        print('-' * 100)
        print(fp)
        stmt_list = query_stmt_split(fp)
        for stmt in stmt_list:
            print('*' * 100)
            print(stmt)
            parser = QueryParser(dict(), is_debug=True)
            try:
                parser.parse(stmt)
                # if parser.binary_join_list and None not in parser.binary_join_list:
                if parser.condition_list:
                    succ += 1
                    query_stmt_list.append((stmt, parser.condition_list))
                else:
                    fail += 1
            except Exception as e:
                print("fail!", e)
                fail += 1
            # query_list.append(query_obj)
            del parser
        print(f"succ: {succ}, fail: {fail}")
    dump(query_stmt_list, open("query_parse_result.pkl", "wb"))
