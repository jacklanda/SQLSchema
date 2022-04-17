import re

if __name__ == "__main__":
    with open("repo_parse_2022_01_18_11:21:30.log", "r") as fp:
        record = dict()
        content = fp.read()
        # all objects
        all_addr_list = re.findall("0x\w{12}", content)
        # runtime object, include lost object
        runtime_addrs = "\n".join(re.findall("table name:.*?0x\w{12}>", content))
        runtime_addr_list = re.findall("0x\w{12}", runtime_addrs)
        # print(len(all_addr_list))
        # print(len(runtime_addr_list))
        # exit()

        for addr in all_addr_list:
            if addr not in record:
                record[addr] = 1
            else:
                record[addr] += 1

        lost_list = list()
        last_list = list()
        for item in record.items():
            if item[1] == 1:
                lost_list.append(item[0])
            elif item[1] > 1:
                last_list.append(item[0])

        print(len(runtime_addr_list))
        print(len(last_list))
        print(len(lost_list))
