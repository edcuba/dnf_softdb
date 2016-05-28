#!/usr/bin/env python3
# Eduard Cuba 2016

import argparse
import os
import sys
import sqlite3

def matcher(col):
    return {
        'nevra':0, 'from_repo':1, 'from_repo_revision':2, 'from_repo_timestamp':3, 'checksum_data':4, 'checksum_type':5,
        'installed_by':6, 'changed_by':7, 'origin_url':8, 'reason':9, 'releasever':10, 'command_line':11,
        'group_member':12, 'tsflag_nocontexts':13
    }[col]

#input argument parser
parser = argparse.ArgumentParser(description="DirectoryDB to SQLite migration tool for yumDB.")
parser.add_argument('-i', '--input', help='Input directory with database structure, default: /var/lib/dnf/yumdb/', default='/var/lib/dnf/yumdb/')
parser.add_argument('-o', '--output', help='output SQLiteDB file, default: ./yumdb.sqlite', default="./yumdb.sqlite")
parser.add_argument('-f', '--force', help='Force overwrite', action='store_true', default=False)
args = parser.parse_args()
no = set(['no','n',''])

#check path to input dir
if not os.path.isdir(args.input):
    sys.stderr.write('Error: Input directory not valid\n')
    exit(1)

#check output file
if os.path.isfile(args.output):
    if not args.force:
        print(args.output + ' will be overwritten, do you want to continue? [Y/n] ')
        choice = input().lower()
        if choice in no:
            print('Interupted by user')
            exit(2)
    os.remove(args.output)

#initialise DB
database = sqlite3.connect(args.output)
cursor = database.cursor()

#create table
cursor.execute('''CREATE TABLE packages (nevra text, from_repo text, from_repo_revision text, from_repo_timestamp text,
                                    checksum_data text, checksum_type text, installed_by text, changed_by text,
                                    origin_url text, reason text, releasever text, command_line text, group_member text,
                                    tsflag_nocontexts text )''')
#scan input dir
for root, dirs, files in os.walk(args.input, topdown=True):
    #scan record dir
    if len(files) > 0:
        record = [''] * 14
        record[matcher('nevra')] = os.path.basename(root)
        for name in files:
            with open(os.path.join(root,name)) as f: record[matcher(name)] =  f.read()
        #insert row into table
        cursor.execute('INSERT INTO packages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record)

#save changes
database.commit()

#close connection
database.close()
