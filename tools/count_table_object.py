
if __name__ == "__main__":
    fpath = "repo_parse_2022_01_18_08:22:28.log"
    table_obj_nums, table_name_nums = 0, 0
    with open(fpath, "r") as fp:
        content = fp.read()
        # table_obj_nums = content.count("s4_parse_sql.Table object")
        # table_name_nums = content.count("table name:")
        table_nums = None
    print("table_obj_nums:", table_obj_nums)
    print("table_name_nums:", table_name_nums)
