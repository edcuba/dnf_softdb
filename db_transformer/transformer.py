#!/usr/bin/env python3
# Eduard Cuba 2016

import argparse
import os
import sys
import sqlite3
import glob

def CONSTRUCT_NAME(row):
    _NAME = row[1] +'-'+ row[3] +'-'+ row[4] +'-'+ row[5]
    return _NAME

def PACKAGE_DATA_INSERT(cursor,data):
    cursor.execute('INSERT INTO PACKAGE_DATA VALUES (null,?,?,?,?,?,?,?,?,?)', data)

def TRANS_DATA_INSERT(cursor,data):
    cursor.execute('INSERT INTO TRANSACTION_DATA VALUES (null,?,?,?,?,?,?,?)', data)

#create binding with repo - returns R_ID
def BIND_REPO(cursor,name):
    cursor.execute('SELECT R_ID FROM REPO WHERE name=?',(name,))
    R_ID = cursor.fetchone()
    if R_ID == None:
        cursor.execute('INSERT INTO REPO VALUES(null,?,0,0)',(name,))
        cursor.execute('SELECT last_insert_rowid()')
        R_ID = cursor.fetchone()
    return R_ID[0]

#create binding with transaction_data table - returns TD_ID
def BIND_TRANS_DATA(h_cursor,PD_ID):
    h_cursor.execute('SELECT * FROM trans_data_pkgs WHERE pkgtups=?',PD_ID)
    #TODO

#input argument parser
parser = argparse.ArgumentParser(description="Unified DNF software database migration tool")
parser.add_argument('-i', '--input', help='DNF yumDB and history folder, default: /var/lib/dnf/', default='/var/lib/dnf/')
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

#initialise historyDB
historyDB = sqlite3.connect(historydb_file)
h_cursor = historyDB.cursor()

#initialise output DB
database = sqlite3.connect(args.output)
cursor = database.cursor()

#value distribution in tables
PACKAGE_DATA = ['P_ID','TD_ID','R_ID','from_repo_revision','from_repo_timestamp',
                    'installed_by','changed_by','installonly','origin_url']
PACKAGE = ['P_ID','name','epoch','version','release','arch','checksum_data','checksum_type','type']
CHECKSUM_DATA = ['checksum_data']
TRANSACTION_DATA = ['T_ID','PD_ID','G_ID','done','ORIGINAL_TD_ID','reason','state']
TRANS_STATE = ['Install','Update','Obsoleting','Updated','Obsoleted']

#create table PACKAGE_DATA
cursor.execute('''CREATE TABLE PACKAGE_DATA (PD_ID integer PRIMARY KEY, P_ID integer, TD_ID text, R_ID integer, from_repo_revision text,
                                    from_repo_timestamp text, installed_by text, changed_by text, installonly text,
                                    origin_url text)''')
#create table PACKAGE
cursor.execute('''CREATE TABLE PACKAGE (P_ID integer, name text, epoch text, version text, release text, arch text, checksum_data text,
                                    checksum_type text, type integer )''')

#create table REPO
cursor.execute('''CREATE TABLE REPO (R_ID INTEGER PRIMARY KEY, name text, last_synced text, is_expired text)''')

#create table TRANSACTION_DATA
cursor.execute('''CREATE TABLE TRANSACTION_DATA (TD_ID INTEGER PRIMARY KEY,T_ID integer,PD_ID integer, G_ID integer ,done INTEGER,
                ORIGINAL_TD_ID integer, reason integer, state integer)''')

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
    cursor.execute('INSERT INTO PACKAGE VALUES (?,?,?,?,?,?,?,?,?)', record_P)

#save changes
database.commit()

#construction of PACKAGE_DATA according to pkg_yumdb
actualPID = 0
record_PD = [''] * len(PACKAGE_DATA)
h_cursor.execute('SELECT * FROM pkg_yumdb')

#for each row in pkg_yumdb
for row in h_cursor:
    newPID = row[0]
    if actualPID != newPID:
        if actualPID != 0:
            record_PD[PACKAGE_DATA.index('P_ID')] = actualPID
            PACKAGE_DATA_INSERT(cursor,record_PD) #insert new record into PACKAGE_DATA
        actualPID = newPID
        record_PD = [''] * len(PACKAGE_DATA)

    if row[1] in PACKAGE_DATA:
        record_PD[PACKAGE_DATA.index(row[1])] = row[2] #collect data for record from pkg_yumdb
    elif row[1] == "from_repo":
        record_PD[PACKAGE_DATA.index("R_ID")] = BIND_REPO(cursor,row[2]) #create binding with REPO table

record_PD[PACKAGE_DATA.index('P_ID')] = actualPID
PACKAGE_DATA_INSERT(cursor,record_PD) #insert last record

#save changes
database.commit()

#transaction_data construction
h_cursor.execute('SELECT * FROM trans_data_pkgs')
for row in h_cursor:
    record_TD = ['']*len(TRANSACTION_DATA)
    record_TD[TRANSACTION_DATA.index('T_ID')] = row[0] #T_ID
    if row[2] == 'TRUE':
        record_TD[TRANSACTION_DATA.index('done')] = 1
    else:
        record_TD[TRANSACTION_DATA.index('done')] = 0
    record_TD[TRANSACTION_DATA.index('state')] = TRANS_STATE.index(row[3])
    pkgtups_tmp = int(row[1])
    cursor.execute('SELECT PD_ID FROM PACKAGE_DATA WHERE P_ID=?',(pkgtups_tmp,))
    pkgtups_tmp = cursor.fetchone()
    if pkgtups_tmp != None:
        record_TD[TRANSACTION_DATA.index('PD_ID')] = pkgtups_tmp[0]
    else:
        task_failed+=1
    task_performed+=1
    TRANS_DATA_INSERT(cursor,record_TD)

#TODO číselník pre state, párovenie ORIGINAL_TD_ID

#save changes
database.commit()



#probably not neccessary
#pkglist = {}
#get package list of yumdb
#for dir in os.listdir(yumdb_path):
#    for subdir in os.listdir(os.path.join(yumdb_path,dir)):
#        pkglist[subdir.partition('-')[2]] = os.path.join(dir,subdir)
#
#fetching aditional values from directory yumdb
#cursor.execute('SELECT * FROM PACKAGE')
#allrows = cursor.fetchall()
#for row in allrows:
#    name = CONSTRUCT_NAME(row)
#    if name in pkglist:
#        record_PD = ['null'] * len(PACKAGE_DATA)
#        path = os.path.join(yumdb_path,pkglist[name])
#        for file in os.listdir(path):
#            if file in PACKAGE_DATA:
#                with open(os.path.join(path,file)) as f: record_PD[PACKAGE_DATA.index(file)] =  f.read()
#            elif file == "from_repo":
#                with open(os.path.join(path,file)) as f: record_PD[PACKAGE_DATA.index("R_ID")] = BIND_REPO(cursor, f.read()) #create binding with REPO table
#        record_PD[PACKAGE_DATA.index('P_ID')] = row[0]
#        print(record_PD)


#            record_nevra= (record_nevra.partition('-')[2]).rsplit('-',3)
#close connection
database.close()
historyDB.close()

if task_performed > 0:
    print("Database integrity " + repr(((task_performed - task_failed) / task_performed)*100) + "%")
else:
    print("Database transformation NOT successfull")
