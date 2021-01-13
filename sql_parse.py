# -*- coding: utf-8 -*-
"""
Created on 09/01/21 7:55 pm

@author : Nirav Langaliya
"""

# File Name : sql_parser_demo.py

# Enter feature description here

# Enter steps here

import re

sql_str = """
INSERT INTO TABLE_TEST_NL
select
     sql_views.id thing_id
     , sql_views.name
     , sql_views.sql
     , 'view' as tpe
from
    periscope_usage_data.sql_views
where
     sql_views.deleted_at is null
union all
select
    charts.id
    , charts.name
    , charts.sql
    , 'chart' as tpe
from
    periscope_usage_data.charts
join periscope_usage_data.dashboards on
    charts.dashboard_id = dashboards.id
where
    charts.deleted_at is null
    and dashboards.deleted_at is null
union all
select
    csvs.id
    , csvs.name
    , '' as sql
    , 'csv' as tpe
from
    periscope_usage_data.csv
"""
sql_str_2 = """
SELECT CASE WHEN A.info IS NULL THEN 'Jam'
            WHEN B.inform LIKE 'N/A%' THEN 'Butter'
            WHEN A.info > 4 THEN 'Apricot'
            ELSE 'Organic'
       END AS information,
       -- More of the same
       FROM 2_KU_4_SKU AS A
       LEFT JOIN
       SKU_HOWS_ROC AS B
       ON A.fskuid = B.idfsk;
INSERT INTO 2_KU_4_SKU
       -- More case statements and other stuff but lets just say
       sku_id AS fskuid,
       short_sku_desc AS info
       FROM BASIC_SKU_TABLE_NOT_A_VIEW_TEDS_VERSION_LATEST_Q108;
INSERT INTO SKU_HOWS_ROC
       -- You guessed it
       sku_id AS idfsk, -- Because words
       sku_blurb AS inform
       FROM BASIC_SKU_TABLE_STILL_NOT_A_VIEW_BILLS_EXCELLENT_VERSION;
"""
REG_BLOCK_COMMENT = re.compile("(/\*)[\w\W]*?(\*/)", re.I)
REG_LINE_COMMENT = re.compile('(--.*)', re.I)
REG_BRACKETS = re.compile("\[|\]|\)|\"", re.I)
REG_PARENTS = re.compile("(?<=join\s)+[\S\.\"\']+|(?<=from\s)+[\S\.\"\']+", re.I)
REG_CTES = re.compile("(\S+)\sas\W*\(", re.I)
#sql_str = sql_str_2
print (sql_str)


def clean_sql(sql):
   c_sql = sql.lower()  # lowercase everything (for easier match)
   c_sql = REG_BLOCK_COMMENT.sub('', c_sql)  # remove block comments
   c_sql = REG_LINE_COMMENT.sub('', c_sql)  # remove line comments
   c_sql = ' '.join(c_sql.split())  # replace \n and multi space w space
   return c_sql

def get_parents(c_sql):
   parents = set(REG_PARENTS.findall(c_sql))
   return parents
# this returns the unique set of ctes per query, so we can exclude them from the list of parents
def get_ctes(c_sql):
   ctes = set(REG_CTES.findall(c_sql))
   return ctes

c_sql = clean_sql(sql_str)
parents = get_parents(c_sql)
cte = get_ctes(c_sql)
print(c_sql)
node_info_dict = {}
parent_dict = {}
ctes = get_ctes(c_sql)
# remove CTES from parent dict
for cte in ctes:
    parents.discard(cte)
# get rid of brackets in views
c_parents = set()
for parent in parents:
    if not parent[:1] == '(':
        c_parents.add(REG_BRACKETS.sub('', parent))
print(c_parents)
object_name = 'test_table'
object_type = 'table'
# add the object name and type and direct parents to the dict
node_info_dict[object_name] = object_type
parent_dict[object_name] = c_parents
    #return (parent_dict, node_info_dict)

def get_child_dict(parent_dict):
   child_dict = {}
   for node in parent_dict:
       print(node)
       for parent in parent_dict[node]:
           print(parent)
           if not parent in child_dict.keys():
               child_dict[parent] = set(node)
           else:
               child_dict[parent].add(node)
   return child_dict

print ( get_child_dict(parent_dict))


from sqllineage.runner import LineageRunner
result = LineageRunner(sql_str)
print(result)

from moz_sql_parser import parse
import json
#print(json.dumps(parse(c_sql)))
