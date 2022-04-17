import os
import pickle

import sqlparse


def judge(stmt):
    # """
    def from_multitables(stmt):
        clause = stmt.split("from")[1].split("where")[0]
        return True if ',' in clause else False

    # if "/*" in stmt:
        # return False

    # return True if "select" in stmt and "from" in stmt else False
    if "select" in stmt and "from" in stmt:
        if "join" in stmt:
            return True
        if "where" in stmt:
            return from_multitables(stmt)
    return False
    # """


if __name__ == "__main__":
    # pkl_fpath = "data/samples/fpath_list_300_2022_01_17_11:59:46.pkl"
    # fpath_list = pickle.load(open(pkl_fpath, "rb"))
    fpath = "all_join_like_stmt_s2.txt"

    split_by_new_line = list()
    ps = list()
    ns = list()
    with open("all_join_like_stmt_ns_s3.txt", "w") as fw_ns:
        with open("all_join_like_stmt_s3.txt", "w") as fw:
            # for fpath in fpath_list:
            with open(fpath, "r", errors="ignore") as fr:
                lines = fr.readlines()
                lines = [l.lower() for l in lines]
                stmt = ""
                for line in lines:
                    if len(line.strip()) == 0:
                        if stmt.strip() != "":
                            split_by_new_line.append(stmt + '\n')
                        stmt = ""
                        continue
                    stmt += line
            ps = [l for l in split_by_new_line if judge(l)]
            ns = [l for l in split_by_new_line if not judge(l)]

            ps_file_content = "".join(ps)
            ns_file_content = "".join(ns)
            fw.write(ps_file_content)
            fw_ns.write(ns_file_content)
    print("done")
