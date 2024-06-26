# SQL Schema Dataset

## Code Structure (Top-Down)
	- Run.sh is the outermost shell script
		○ Invokes `repo_parse_sql.py` 
		○ (Need to comment/uncomment pre-aggregated repo files in `repo_parse_sql.py`
			1% of data: `/datadrive/yang/exp/data/samples/repo_list_11k.pkl` (storing all repos with aggregated info of SQL files in the same repo, 11K=>1% sample, and 970K=>100%)
			Alternatively, the full list of repos: `Please stay tuned for further release!`

	- `repo_parse_sql.py` is the outermost Python file, serves as the master, that calls parallel workers implemented in this file
		○ It calls `s4_parse_sql.py`
    
	- `s4_parse_sql.py` is the middle layer, which has "class File" level logic (from my old code), performs multi-stage parsing (first process create-table, then alter-table, then FK, then query, etc.). 
		○ This calls out to `parse_query.py` for per query parsing
    
	- `parse_query.py` is the lowest layer, for per query parsing (Join, aggregate, groupby, etc.)

## Other Notes:
[[Link to Code](https://github.com/jacklanda/bert4sql/blob/8436ed8c8305cc1c3bd5125bfa387477fa919dbf/sql_parse/repo_parse_sql.py#L26)]
Even though there is a `join_query_list`, it stores all Query info (not just join but also agg, selection, etc.)

# Code Instruction from Yang: 
[[Code Instruction](https://alpine-seat-ecd.notion.site/Code-Instruction-015c2ed396394598a7953d5e3f765b5d)] (Unavailable for now.)

# Third-Party Library Dependencies

- pebble: A thread parallel library with timeout settings
- sqlparse: SQL query statement ⇒ AST
- sql_metadata: Further encapsulation of sqlparse, facilitating the use of AST
- beautifulsoup4: Uses its internal method UnicodeDammit to detect the encoding of SQL script

# Project Structure

- `data/`
    - `s3_sql_files_crawled_all_vms/` ⇒ Stores all decompressed SQL scripts text
    - `s4_sql_files_parsed/` ⇒ Stores parse output .pkl files
    - `samples/` ⇒ Stores .pkl files for sample analysis
    - `variants_cases/` ⇒ Stores results matched through grep from multiple sources
- `log/`
- `sql_parse/`
    - `cls_def.py`
        - Defines multiple basic classes including Key, ForeignKey, Index, Column, Table, Pipeline, and their respective class methods.
    - `display.py`
        - Reads a .pkl file, counts and prints the main contents of the pkl
    - `dump_tables.py`
        - Reads a .pkl file, outputs the content in the sequence form required for Language Modeling
    - exceptions.py (unused)
        - Custom exception classes
    - `parse_query.py`
        - Parsing classes for join and non-join queries, module defines TableInstance, BinaryJoin, Query, QueryNode, QueryTree, TokenVisitor, QueryParser classes, and their respective class methods.
        - Each input query statement will construct a QueryParser object in the calling function and call its parse method to parse the input query statement.
        - Constructs a Query Tree for each query statement, where each node in the tree corresponds to each query scope in the query statement. Query nodes record member variables pointing to their parent node (if any), child nodes (if any), corresponding sub-query statement, and sub-query AST.
        - Parses the query tree in the order of join condition -> projection -> aggregation -> selection -> groupby and saves the results to the corresponding member variables under QueryParser.
        - For join condition, projection, aggregation, selection, and groupby, any one of them needs to parse and check successfully to consider the query statement parsed successfully, and a query object will be constructed and saved for that.
    - `query_sample.py`
        - This module defines practical functions for analyzing and calculating the overlap ratio of table fails, column fails, FK, and join condition overlaps, among others.
    - `repo_parse_sql.py`
        - This module defines the Repository class and related functions for aggregating repo processing. Generally, this module serves as the entry module called by the shell script. In parallel processing conditions, this module serves as the master thread responsible for scheduling each worker thread forked.
    - `s4_parse_sql.py`
        - In parallel processing conditions, the module defines the function parse_repo_files as the calling function for each worker.
    - `s4c_post_process_tables_for_training.py`
        - Input a pickle file, perform post-processing on the data
    - `sample.py`
        - This module defines a series of practical functions for printing table objects, column objects, etc.
    - `utils.py`
        - Defines a custom class RegexDict for managing regular expressions used in extraction, a custom class for mapping column categories used in conversion, and also implements some common functions used in the parsing process.
- `tools/`
    - `grep_join_like_stmts.py`: Calculates the number of join query statements in all SQL scripts

# Parse Function Introduction

Execute parsing process by passing different parameters to the run.sh script.

OPTIONS:<br>
-h, --help show a list of command-line options<br>
-t, --test unit test for shell script functions<br>
-p, --parse fork a SQL parse process<br>
-d, --debug debug SQL parse scripts with pudb<br>

SQL parse has two modes: parallel processing and serial debugging mode.

Enter (parallel) processing mode with the `-p` / `--parse` option:
After starting parsing, a new directory will be created under `Please stay tuned for further release!`
(ending with the time format `yyyy_mm_dd_hr:mi:se` when the task is started).
To avoid potential memory overflow issues during parsing, the completed .pkl files will be saved in batches in this directory.
Note: Batch saved files have _d numeric suffixes as filenames;
When all parsing is completed, the program will merge and store all .pkl files into a unified .pkl file in this directory.

p.s. 1) The merged file has no _d numeric suffix in the filename;
     2) If you need to parse the full dataset, uncomment line 321 in repo_parse_sql.py in advance;
     3) If you need to parse 1/100 of the full random sample data, uncomment line 321 in `repo_parse_sql.py` in advance,
        and uncomment line 322 in `repo_parse_sql.py`.

Enter (serial) debugging mode with `-d` / `--debug`: Debug mode parsing output is explained as above.


# Data Analysis

### Dataset Statistical Analysis

- Analyze the overall pkl file output:

    ```markdown
    Use the display.py script to perform statistical analysis on the pkl files output by the entire parse process. The measurable data includes:
    - Total number of non-empty repositories,
    - Total number of parsed tables,
    - Total number of non-empty tables,
    - Total number of columns in all tables,
    - Total number of primary keys in all tables,
    - Total number of foreign keys in all tables,
    - Number of unique constraints,
    - Number of candidate keys,
    - Number of columns with data types,
    - Total number of queries,
    - Total number of binary joins,
    - Total number of join conditions,
    - Total number of indices,
    - Number of queries with projection,
    - Number of queries with aggregation,
    - Number of queries with selection,
    - Number of queries with groupby,
    ```

- Methods for analyzing tables with the same name:
    - Use the `print_name2tab` function in `s4_parse_sql.py` to output tables with the same name while running a repository data (can be done in parallel or serial).
- Analysis method for overlapping ForeignKey and BinaryJoin Condition:
    - In `query_sample.py`, use the `calc_fk_jq_overlap` function with a processed `repo_list` (read from pkl file), and the printed result after function execution is the analysis result of whether they overlap.
- Proportion of missing tables in the same user's repository:
    - Use the `calc_missing_table_in_other_repo` function in `query_sample.py` with a processed `repo_list` (read from pkl file) to print the proportion of missing tables in join queries in other repositories under the same user.
- Count the number of table check fail and column check fail:
    - Use the `calc_failed_cases_num` function in `query_sample.py` with a processed `repo_list` (read from pkl file) to print the quantities of all check failed cases, table check failed cases, and column check failed cases.
- Output pkl file content as serialized schema sequence:
    - Run the `dump_tables.py` script to output the data in the pkl file as natural language sentence-formatted table schema sequences.
- Check the entity information saved in the pkl file:
    - Print the content saved in the pkl file for each level of objects. Use functions like `print_query_obj`, `print_table_obj`, etc. from the `sample.py` module to pass the corresponding level of objects and print the saved entity information.

# Debug Method

> Debugging for this project is done using Python 3rd party tool PuDB.
>
- Debugging for a specific user:
    - Uncomment line 288 in `repo_parse_sql.py` to aggregate repos belonging to the same user

- Debugging for a specific repository:
    - Change the repo URL to be debugged at line 291 in `repo_parse_sql.py`, set breakpoints, and start debugging in the `Please stay tuned for further release!` directory using `sh run.sh -d`.
- Debugging for a specific SQL file:
    - Change the file path of the SQL file to be debugged at line 1554 in `s4_parse_sql.py`, set breakpoints, and start debugging in the `/datadrive/yang/exp` directory using `sh run.sh -d`.
- Debugging for a specific statement:
    - For queries, insert the statement to be parsed at `parse_query.py:2219`, set breakpoints, and start debugging in the `Please stay tuned for further release!` directory using `sh run.sh -d`.

# Citation (BibTex)
Please cite this repository by the following bibtex, if you feel this project is helpful to your research.
```latex
@misc{sql-schema-dataset,
    title={SQL Schema Dataset},
    author={Yang Liu},
    url={https://github.com/jacklanda/SQLSchema},
    howpublished={\url{https://github.com/jacklanda/SQLSchema}},
    year={2022}
}
```
