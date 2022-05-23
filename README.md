
# summary from the meeting on code:

## code structure (top-down)
	- Run.sh is the outer-most shell script
		○  Invokes repo_parse_sql.py 
		○ (need to comment/uncomment pre-aggregated repo files,  in repo_parse_sql.py
			1% of data: /datadrive/yang/exp/data/samples/repo_list_11k.pkl  (storing all repos with aggregated info of sql files in same repo, 11K=>1% sample, and 970K=>100%) 
			Alternatively,  full list of repos:  /datadrive/yang/exp/data/samples/repo_list_all.pkl 
      
	- repo_parse_sql.py is the outer-most python file, serves as the master, that calls parallel workers implemented in this file
		○ It calls s4_parse_sql.py
    
	- s4_parse_sql.py is the middle layer, that has "class File" level logic (from my old code), performs multi-stage parsing (first process create-table, then alter-table, then FK, then query, etc.). 
		○ This calls out to parse_query.py, for per query parsing
    
	- parse_query.py is the lowest layer, for per query parsing (Join, aggregate, groupby, etc.)


## other notes:
https://github.com/jacklanda/bert4sql/blob/8436ed8c8305cc1c3bd5125bfa387477fa919dbf/sql_parse/repo_parse_sql.py#L26
Even though there is a "join_query_list", it stores all Query info (not just join, but also agg, selection, etc.)
