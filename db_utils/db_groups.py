#!/usr/bin/env python3
# Eduard Cuba 2016

import argparse
import os
import sys
import sqlite3
import json


#input argument parser
parser = argparse.ArgumentParser(description="SW DB groups.json parser")
parser.add_argument('-i', '--input', help='groups.json path, default: ./groups.json', default='./groups.json')
parser.add_argument('-o', '--output', help='SW_DB path, default: ./sw_db.sqlite', default='./sw_db.sqlite')
args = parser.parse_args()

sw_db_path = args.output
groups_path = args.input

GROUPS = ['name','ui_name','is_installed','exclude','full_list','pkg_types','grp_types']

#check path to sw_db file
if not os.path.isfile(sw_db_path):
    sys.stderr.write('Error: sw_db file not valid\n')
    exit(1)

#check path to groups file
if not os.path.isfile(groups_path):
    sys.stderr.write('Error: groups file not valid\n')
    exit(1)

#initialise SW DB
database = sqlite3.connect(sw_db_path)
cursor = database.cursor()

#cursor.execute('''CREATE TABLE GROUPS (G_ID INTEGER PRIMARY KEY, name text, ui_name text, is_installed integer, exclude text,
#                full_list text, pkg_types integer, grp_types integer)''')

with open(groups_path) as groups_file: data = json.load(groups_file)
for key in data:
    if key != 'meta':
        record_G = [''] * len(GROUPS)
        for value in data[key]:
            record_G[GROUPS.index('ui_name')] = key
            record_G[GROUPS.index('name')] = value
            for row in data[key][value]:
                record_G[GROUPS.index('exclude')] = str(data[key][value]['pkg_exclude'])
                record_G[GROUPS.index('pkg_types')] = data[key][value]['pkg_types']
                record_G[GROUPS.index('grp_types')] = data[key][value]['grp_types']
                record_G[GROUPS.index('full_list')] = str(data[key][value]['full_list'])
                record_G[GROUPS.index('is_installed')] = 1
            cursor.execute('INSERT INTO GROUPS VALUES (null,?,?,?,?,?,?,?)',(record_G))

database.commit()

#close connection
database.close()
