import re

if __name__ == "__main__":
    fpath = "repo_parse_2022_01_19_07:34:40.log"
    # fpath = "repo_parse_2022_01_18_02:22:56.log"
    parsed_repo_count = 0
    total_count, succ_count, except_count = 0, 0, 0
    with open(fpath, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            if "create table succ" in line:
                res = re.findall(r"\d+", line)
                if len(res) != 3:
                    continue
                total_tmp = int(res[0])
                succ_tmp = int(res[1])
                excp_tmp = int(res[2])
                total_count += total_tmp
                succ_count += succ_tmp
                except_count += excp_tmp
                # total_count = total_tmp if total_tmp > total_count else total_tmp
                # succ_count = succ_tmp if succ_tmp > succ_count else succ_count
                # except_count = excp_tmp if excp_tmp > except_count else except_count
            elif "repo parse done" in line:
                parsed_repo_count += 1
    print(f"succ count: {succ_count}")
    print(f"total input create table stmts: {total_count}")
    print(f"succ cov: {succ_count/total_count}")
    print(f"parsed repo nums: {parsed_repo_count}")
