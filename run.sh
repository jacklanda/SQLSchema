#!/usr/bin/env sh
# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com

arg_num=$#;
fn_slt=$1;

if [ $arg_num -lt 1  ]; then
    echo "usage:\n  sh run.sh [options]"
    exit 0
fi

localtime=`date +"%Y_%m_%d_%H:%M:%S"`
sql_parse_log_path="repo_parse_${localtime}.log"

help(){
    echo "OPTIONS"
    echo "  -h, --help\tshow list of command-line options"
    echo "  -t, --test\tunit test for shell script functions"
    echo "  -p, --parse\tfork a SQL parse process"
}

sql_parse(){
    echo "sql parse begin, log: ${sql_parse_log_path}"
    nohup python sql_parse/repo_parse_sql.py > ${sql_parse_log_path} 2>&1 &
}

sql_parse_debug(){
    echo "sql parse debug"
    python sql_parse/repo_parse_sql.py
}

unit_test(){
    echo "input args: $fn_slt"
    echo "local time: ${localtime}"
    echo "sql parse log path: ${sql_parse_log_path}"
}

main(){
    case $fn_slt in
        "-h" | "--help") 
            help
            ;;
        "-t" | "--test")
            unit_test
            ;;
        "-p" | "--parse") 
            sql_parse
            ;;
        "-d" | "--debug")
            sql_parse_debug
            ;;
        *) echo "invalid arg: ${fn_slt}"
    esac
}

main
