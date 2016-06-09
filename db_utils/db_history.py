#!/usr/bin/env python3
# Eduard Cuba 2016
# FIXME: PACKAGE:PACKAGE_DATA ratio is 2:1
#       - create empty PACKAGE_DATA only for binding with TRANS_DATA?

import argparse
import os
import sys
import sqlite3
import datetime

def print_format(*args):
    print('{:>7} | {:<24}| {:<16} | {:<15}|{:^8} '.format(*args))


################################ INPUT PARSER #################################

#input argument parser
parser = argparse.ArgumentParser(description="Unified DNF software database utils")
parser.add_argument('-i', '--input', help='SW_DB path, default: ./sw_db.sqlite', default='./sw_db.sqlite')
args = parser.parse_args()

sw_db_path = args.input

#check path to sw_db file
if not os.path.isfile(sw_db_path):
    sys.stderr.write('Error: sw_db file not valid\n')
    exit(1)

#initialise SW DB
database = sqlite3.connect(sw_db_path)
cursor = database.cursor()

cursor.execute('SELECT * FROM TRANS')
transactions = cursor.fetchall()
print_format('ID',' Command Line',' Date and Time',' Action(s)', ' Altered')
print('-----------------------------------------------------------------------------')
for transaction in transactions[::-1]:
    output = [''] * 5
    output[0] = transaction[0]
    output[1] = (transaction[4])[:24]
    output[2] = str(datetime.datetime.fromtimestamp(int(transaction[1])))[:16]
    cursor.execute('SELECT state FROM TRANS_DATA WHERE T_ID = ?',(transaction[0],))
    all_actions = cursor.fetchall()
    altered = 0
    for action in all_actions:
        cursor.execute('SELECT description FROM STATE_TYPE WHERE id=?',(action[0],))
        action = cursor.fetchone()
        if action[0] != 'Installed' and action[0] != 'Updated':
            altered+=1
    output[4] = altered
    actions = list(set(all_actions))
    u_actions = []
    for action in actions:
        cursor.execute('SELECT description FROM STATE_TYPE WHERE id=?',(action[0],))
        action = cursor.fetchone()
        u_actions.append(action[0])
    if 'Updated' in u_actions:
        del u_actions[u_actions.index('Updated')]
    if 'Obsoleted' in u_actions:
        del u_actions[u_actions.index('Obsoleted')]
    if len(u_actions) == 1:
        output[3] = u_actions[0]
    else:
        u_actions = sorted(u_actions)
        for action in u_actions:
            if output[3] == '':
                output[3] = action[0]
            else:
                output[3] += ', '+action[0]

    print_format(*output)




#close connection
database.close()
