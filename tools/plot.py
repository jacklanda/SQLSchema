# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com


import re
from sys import argv
from pprint import pprint

import plotext as plt


def draw(file_name="repo_parse.log"):
    plot_list = list()
    with open(file_name, "r") as fp:
        lines = fp.readlines()
        # lines = [l.strip() for l in lines if "succ:" in l and "except:" in l]
        lines = [l.strip() for l in lines if "query_succ:" in l and "query_except:" in l]
    for line in lines:
        # extract target => succ: 400771, except: 33507
        # result = re.search("succ:\s(\d+),\sexcept:\s(\d+)", line)
        result = re.search("query_succ:\s(\d+),\squery_except:\s(\d+)", line)
        _succ, _except = int(result.group(1)), int(result.group(2))
        try:
            cov = _succ / (_succ + _except)
        except:
            continue
        plot_list.append(cov)
    # plt.plot(plot_list, color="magenta")
    plt.plot(plot_list, color="red")
    plt.frame(True)
    plt.grid(True)
    plt.ylim(0.0, 1.0)
    plt.xscale("iter")
    plt.yscale("cov")
    plt.title("SQL Parse Coverage")
    plt.show()
    pprint(plot_list[-10:])


if __name__ == "__main__":
    log_path = argv[1]
    draw(log_path)
