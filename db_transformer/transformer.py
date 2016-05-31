#!/usr/bin/env python3
# Eduard Cuba 2016

import argparse
import os
import sys
import sqlite3
import glob

def PD_ID_gen():
    PD_ID_index = 1
    while True:
        yield PD_ID_index
        PD_ID_index += 1

#input argument parser
parser = argparse.ArgumentParser(description="Unified DNF software database migration tool")
parser.add_argument('-i', '--input', help='DNF yumDB and history folder: /var/lib/dnf/', default='/var/lib/dnf/')
parser.add_argument('-o', '--output', help='output SQLiteDB file, default: ./sw_db.sqlite', default="./sw_db.sqlite")
parser.add_argument('-f', '--force', help='Force overwrite', action='store_true', default=False)
args = parser.parse_args()
no = set(['no','n',''])

yumdb_path = os.path.join(args.input,'yumdb')
history_path = os.path.join(args.input,'history')

#check path to yumdb dir
if not os.path.isdir(yumdb_path):
    sys.stderr.write('Error: yumdb directory not valid\n')
    exit(1)

#check path to history dir
if not os.path.isdir(history_path):
    sys.stderr.write('Error: history directory not valid\n')
    exit(1)

#check historyDB file and pick newest one
historydb_file = glob.glob(os.path.join(history_path,"history*"))
if len(historydb_file) < 1:
    sys.stderr.write('Error: history database file not valid\n')
    exit(1)
historydb_file.sort()
historydb_file = historydb_file[0]

#check output file
if os.path.isfile(args.output):
    if not args.force:
        print(args.output + ' will be overwritten, do you want to continue? [Y/n] ')
        choice = input().lower()
        if choice in no:
            print('Interupted by user')
            exit(2)
    os.remove(args.output)

#initialise variables
task_performed = 0
task_failed = 0

#intialise IDs
PD_ID = PD_ID_gen()

#initialise historyDB
historyDB = sqlite3.connect(historydb_file)
h_cursor = historyDB.cursor()

#initialise output DB
database = sqlite3.connect(args.output)
cursor = database.cursor()

#value distribution in tables
PACKAGE_DATA = ['PD_ID','P_ID','TD_ID','R_ID','from_repo_revision','from_repo_timestamp',
                    'installed_by','changed_by','installonly','origin_url']
PACKAGE = ['P_ID','name','epoch','version','release','arch','checksum_data','checksum_type','type']
CHECKSUM_DATA = ['checksum_data']


#create table PACKAGE_DATA
cursor.execute('''CREATE TABLE PACKAGE_DATA (PD_ID integer, P_ID integer, TD_ID text, R_ID text, from_repo_revision text,
                                    from_repo_timestamp text, installed_by text, changed_by text, installonly text,
                                    origin_url text)''')
#create table PACKAGE
cursor.execute('''CREATE TABLE PACKAGE (P_ID integer, name text, epoch text, version text, release text, arch text, checksum_data text,
                                    checksum_type text, type integer )''')

#contruction of PACKAGE from pkgtups
h_cursor.execute('SELECT * FROM pkgtups')
for row in h_cursor:
    record_P = [''] * len(PACKAGE) #init
    record_P[0] = row[0] #P_ID
    record_P[1] = row[1] #name
    record_P[2] = row[3] #epoch
    record_P[3] = row[4] #version
    record_P[4] = row[5] #release
    record_P[5] = row[2] #arch
    record_P[6] = row[6].split(":",2)[1] #checksum_data
    record_P[7] = row[6].split(":",2)[0] #checksum_type
    record_P[8] = 1 #type
    try:
        cursor.execute('INSERT INTO PACKAGE VALUES (?,?,?,?,?,?,?,?,?)', record_P)
    except:
        task_failed+=1
    task_performed+=1

#save changes
database.commit()

#construction of PACKAGE_DATA and matching with PACKAGE
for root, dirs, files in os.walk(yumdb_path, topdown=True):
    if len(files) > 0:
        record_PD = [''] * len(PACKAGE_DATA)
        record_CS = ''
        record_PD[PACKAGE_DATA.index('PD_ID')] = next(PD_ID)
        for name in files:

            #determine usage of found file
            if name in PACKAGE_DATA:
                with open(os.path.join(root,name)) as f: record_PD[PACKAGE_DATA.index(name)] =  f.read()
            elif name in CHECKSUM_DATA:
                with open(os.path.join(root,name)) as f: record_CS =  f.read()

        #pair with PACKAGE via checksum
        cursor.execute('SELECT P_ID FROM PACKAGE WHERE checksum_data=?',(record_CS,))
        match = cursor.fetchone()

        #no instance with same checksum_data in PACKAGE
        if match != None :
            record_PD[PACKAGE_DATA.index('P_ID')] = match[0]
        else:
            record_PD[PACKAGE_DATA.index('P_ID')] = 0
            task_failed += 1

        task_performed+=1

        #insert rows into tables
        try:
            cursor.execute('INSERT INTO PACKAGE_DATA VALUES (?,?,?,?,?,?,?,?,?,?)', record_PD)
        except:
            task_failed +=1
        task_performed+=1




#save changes in PACKAGE_DATA and PACKAGE
database.commit()

#save changes
#database.commit()

#close connection
database.close()
historyDB.close()

if task_performed > 0:
    print("Database integrity " + repr(((task_performed - task_failed) / task_performed)*100) + "%")
else:
    print("Database transformation NOT successfull")
