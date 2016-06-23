#!/usr/bin/env python3
# Eduard Cuba 2016

import argparse
import os
import sys
import sqlite3
import glob
import json

################################# FUNCTIONS ###################################

def CONSTRUCT_NAME(row):
    _NAME = row[1] +'-'+ row[3] +'-'+ row[4] +'-'+ row[5]
    return _NAME

def PACKAGE_DATA_INSERT(cursor,data):
    cursor.execute('INSERT INTO PACKAGE_DATA VALUES (null,?,?,?,?,?,?,?,?)', data)

def TRANS_DATA_INSERT(cursor,data):
    cursor.execute('INSERT INTO TRANS_DATA VALUES (null,?,?,?,?,?,?,?)', data)

def TRANS_INSERT(cursor,data):
    cursor.execute('INSERT INTO TRANS VALUES (?,?,?,?,?,?,?,?)', data)

#create binding with repo - returns R_ID
def BIND_REPO(cursor,name):
    cursor.execute('SELECT R_ID FROM REPO WHERE name=?',(name,))
    R_ID = cursor.fetchone()
    if R_ID == None:
        cursor.execute('INSERT INTO REPO VALUES(null,?,0,0)',(name,))
        cursor.execute('SELECT last_insert_rowid()')
        R_ID = cursor.fetchone()
    return R_ID[0]

#create binding with STATE_TYPE - returns ID
def BIND_STATE(cursor,desc):
    cursor.execute('SELECT ID FROM STATE_TYPE WHERE description=?',(desc,))
    STATE_ID = cursor.fetchone()
    if STATE_ID == None:
        cursor.execute('INSERT INTO STATE_TYPE VALUES(null,?)',(desc,))
        cursor.execute('SELECT last_insert_rowid()')
        STATE_ID = cursor.fetchone()
    return STATE_ID[0]

#create binding with REASON_TYPE - returns ID
def BIND_REASON(cursor,desc):
    cursor.execute('SELECT ID FROM REASON_TYPE WHERE description=?',(desc,))
    REASON_ID = cursor.fetchone()
    if REASON_ID == None:
        cursor.execute('INSERT INTO REASON_TYPE VALUES(null,?)',(desc,))
        cursor.execute('SELECT last_insert_rowid()')
        REASON_ID = cursor.fetchone()
    return REASON_ID[0]

#create binding with OUTPUT_TYPE - returns ID
def BIND_OUTPUT(cursor,desc):
    cursor.execute('SELECT ID FROM OUTPUT_TYPE WHERE description=?',(desc,))
    OUTPUT_ID = cursor.fetchone()
    if OUTPUT_ID == None:
        cursor.execute('INSERT INTO OUTPUT_TYPE VALUES(null,?)',(desc,))
        cursor.execute('SELECT last_insert_rowid()')
        OUTPUT_ID = cursor.fetchone()
    return OUTPUT_ID[0]

def BIND_PACKAGE(cursor,desc):
    cursor.execute('SELECT ID FROM PACKAGE_TYPE WHERE description=?',(desc,))
    PACKAGE_ID = cursor.fetchone()
    if PACKAGE_ID == None:
        cursor.execute('INSERT INTO PACKAGE_TYPE VALUES(null,?)',(desc,))
        cursor.execute('SELECT last_insert_rowid()')
        PACKAGE_ID = cursor.fetchone()
    return PACKAGE_ID[0]


#######integrity optimalization######
def BIND_PID_PDID(cursor,pid):
    cursor.execute('SELECT PD_ID FROM PACKAGE_DATA WHERE P_ID=?',(pid,))
    PPD_ID = cursor.fetchone()
    if PPD_ID == None:
        cursor.execute('INSERT INTO PACKAGE_DATA VALUES(null,?,?,?,?,?,?,?,?)',(pid,'','','','','','',''))
        cursor.execute('SELECT last_insert_rowid()')
        PPD_ID = cursor.fetchone()
    return PPD_ID[0]
#######integrity optimalization######

#####################################YUMDB#####################################
def GET_YUMDB_PACKAGES(cursor):
    pkglist = {}
    #get package list of yumdb
    for dir in os.listdir(yumdb_path):
        for subdir in os.listdir(os.path.join(yumdb_path,dir)):
            pkglist[subdir.partition('-')[2]] = os.path.join(dir,subdir)
    #fetching aditional values from directory yumdb
    cursor.execute('SELECT * FROM PACKAGE')
    allrows = cursor.fetchall()
    for row in allrows:
        name = CONSTRUCT_NAME(row)
        if name in pkglist:
            record_PD = [None] * len(PACKAGE_DATA)
            path = os.path.join(yumdb_path,pkglist[name])
            for file in os.listdir(path):
                if file in PACKAGE_DATA:
                    with open(os.path.join(path,file)) as f: record_PD[PACKAGE_DATA.index(file)] =  f.read()
                elif file == "from_repo":
                    #create binding with REPO table
                    with open(os.path.join(path,file)) as f: record_PD[PACKAGE_DATA.index("R_ID")] = BIND_REPO(cursor, f.read())
            actualPDID = BIND_PID_PDID(cursor,row[0])
            if record_PD[PACKAGE_DATA.index('R_ID')]:
                cursor.execute('UPDATE PACKAGE_DATA SET R_ID=? WHERE PD_ID=?',(record_PD[PACKAGE_DATA.index('R_ID')],actualPDID))
            if record_PD[PACKAGE_DATA.index('from_repo_revision')]:
                cursor.execute('UPDATE PACKAGE_DATA SET from_repo_revision=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('from_repo_revision')],actualPDID))
            if record_PD[PACKAGE_DATA.index('from_repo_timestamp')]:
                cursor.execute('UPDATE PACKAGE_DATA SET from_repo_timestamp=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('from_repo_timestamp')],actualPDID))
            if record_PD[PACKAGE_DATA.index('installed_by')]:
                cursor.execute('UPDATE PACKAGE_DATA SET installed_by=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('installed_by')],actualPDID))
            if record_PD[PACKAGE_DATA.index('changed_by')]:
                cursor.execute('UPDATE PACKAGE_DATA SET changed_by=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('changed_by')],actualPDID))
            if record_PD[PACKAGE_DATA.index('installonly')]:
                cursor.execute('UPDATE PACKAGE_DATA SET installonly=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('installonly')],actualPDID))
            if record_PD[PACKAGE_DATA.index('origin_url')]:
                cursor.execute('UPDATE PACKAGE_DATA SET origin_url=? WHERE PD_ID=?',
                (record_PD[PACKAGE_DATA.index('origin_url')],actualPDID))

################################ INPUT PARSER #################################

#input argument parser
parser = argparse.ArgumentParser(description="Unified DNF software database migration tool")
parser.add_argument('-i', '--input', help='DNF yumDB and history folder, default: /var/lib/dnf/', default='/var/lib/dnf/')
parser.add_argument('-o', '--output', help='output SQLiteDB file, default: ./sw_db.sqlite', default="./sw_db.sqlite")
parser.add_argument('-f', '--force', help='Force overwrite', action='store_true', default=False)
args = parser.parse_args()
no = set(['no','n',''])

yumdb_path = os.path.join(args.input,'yumdb')
history_path = os.path.join(args.input,'history')
groups_path = os.path.join(args.input,'groups.json')

#check path to yumdb dir
if not os.path.isdir(yumdb_path):
    sys.stderr.write('Error: yumdb directory not valid\n')
    exit(1)

#check path to history dir
if not os.path.isdir(history_path):
    sys.stderr.write('Error: history directory not valid\n')
    exit(1)

do_groups = 1
#check groups path
if not os.path.isfile(groups_path):
    do_groups = 0

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

############################ GLOBAL VARIABLES #################################

print('Database transformation running')

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
PACKAGE_DATA = ['P_ID','R_ID','from_repo_revision','from_repo_timestamp',
                    'installed_by','changed_by','installonly','origin_url']
PACKAGE = ['P_ID','name','epoch','version','release','arch','checksum_data','checksum_type','type']
CHECKSUM_DATA = ['checksum_data']
TRANS_DATA = ['T_ID','PD_ID','G_ID','done','ORIGINAL_TD_ID','reason','state']
TRANS = ['T_ID','beg_timestamp','end_timestamp','RPMDB_version','cmdline','loginuid','releasever','return_code']
GROUPS = ['name','ui_name','is_installed','exclude','full_list','pkg_types','grp_types']

############################# TABLE INIT ######################################

#create table PACKAGE_DATA
cursor.execute('''CREATE TABLE PACKAGE_DATA (PD_ID integer PRIMARY KEY, P_ID integer, R_ID integer,
            from_repo_revision text, from_repo_timestamp text, installed_by text, changed_by text, installonly text, origin_url text)''')
#create table PACKAGE
cursor.execute('''CREATE TABLE PACKAGE (P_ID integer, name text, epoch text, version text, release text, arch text,
            checksum_data text, checksum_type text, type integer )''')

#create table REPO
cursor.execute('''CREATE TABLE REPO (R_ID INTEGER PRIMARY KEY, name text, last_synced text, is_expired text)''')

#create table TRANS_DATA
cursor.execute('''CREATE TABLE TRANS_DATA (TD_ID INTEGER PRIMARY KEY,T_ID integer,PD_ID integer, G_ID integer,
            done INTEGER, ORIGINAL_TD_ID integer, reason integer, state integer)''')

#create table TRANS
cursor.execute('''CREATE TABLE TRANS (T_ID integer, beg_timestamp text, end_timestamp text, RPMDB_version text,
            cmdline text, loginuid integer, releasever text, return_code integer)''')

#create table OUTPUT
cursor.execute('''CREATE TABLE OUTPUT (T_ID INTEGER, msg text, type integer)''')

#create table STATE_TYPE
cursor.execute('''CREATE TABLE STATE_TYPE (ID INTEGER PRIMARY KEY, description text)''')

#create table REASON_TYPE
cursor.execute('''CREATE TABLE REASON_TYPE (ID INTEGER PRIMARY KEY, description text)''')

#create table OUTPUT_TYPE
cursor.execute('''CREATE TABLE OUTPUT_TYPE (ID INTEGER PRIMARY KEY, description text)''')

#create table PACKAGE_TYPE
cursor.execute('''CREATE TABLE PACKAGE_TYPE (ID INTEGER PRIMARY KEY, description text)''')

#create table GROUPS
cursor.execute('''CREATE TABLE GROUPS (G_ID INTEGER PRIMARY KEY, name text, ui_name text, is_installed integer, exclude text,
                full_list text, pkg_types integer, grp_types integer)''')

#create table TRANS_GROUP_DATA
cursor.execute('''CREATE TABLE TRANS_GROUP_DATA (TG_ID INTEGER PRIMARY KEY, T_ID integer, G_ID integer, name text, ui_name text,
                is_installed integer, exclude text, full_list text, pkg_types integer, grp_types integer)''')

################################ DB CONSTRUCTION ##############################

print('Transforming packages')

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
    if row[6]:
        record_P[6] = row[6].split(":",2)[1] #checksum_data
        record_P[7] = row[6].split(":",2)[0] #checksum_type
    record_P[8] = BIND_PACKAGE(cursor,'rpm') #type
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

#fetch from yumdb
GET_YUMDB_PACKAGES(cursor)

#########integrity optimalization#########
cursor.execute('SELECT P_ID FROM PACKAGE')
tmp_row = cursor.fetchall()
for row in tmp_row:
    BIND_PID_PDID(cursor,int(row[0]))
##########################################

#save changes
database.commit()

print('Transforming transactions')
#trans_data construction
h_cursor.execute('SELECT * FROM trans_data_pkgs')
for row in h_cursor:
    record_TD = ['']*len(TRANS_DATA)
    record_TD[TRANS_DATA.index('T_ID')] = row[0] #T_ID
    if row[2] == 'TRUE':
        record_TD[TRANS_DATA.index('done')] = 1
    else:
        record_TD[TRANS_DATA.index('done')] = 0
    record_TD[TRANS_DATA.index('state')] = BIND_STATE(cursor,row[3])
    pkgtups_tmp = int(row[1])
    cursor.execute('SELECT PD_ID FROM PACKAGE_DATA WHERE P_ID=?',(pkgtups_tmp,))
    pkgtups_tmp = cursor.fetchone()
    if pkgtups_tmp != None:
        record_TD[TRANS_DATA.index('PD_ID')] = pkgtups_tmp[0]
    else:
        task_failed+=1
    task_performed+=1
    TRANS_DATA_INSERT(cursor,record_TD)

#save changes
database.commit()

#resolve STATE_TYPE
cursor.execute('SELECT * FROM STATE_TYPE')
state_types = cursor.fetchall()
fsm_state = 0
obsoleting_t = 0
update_t = 0
downgrade_t = 0
for a in range(len(state_types)):
    if state_types[a][1] == 'Obsoleting':
        obsoleting_t = a+1
    elif state_types[a][1] == 'Update':
        update_t = a+1
    elif state_types[a][1] == 'Downgrade':
        downgrade_t = a+1

#find ORIGINAL_TD_ID for Obsoleting and upgraded - via FSM
previous_TD_ID = 0
cursor.execute('SELECT * FROM TRANS_DATA')
tmp_row = cursor.fetchall()
for row in tmp_row:
    if fsm_state == 0:
        if row[7] == obsoleting_t:
            fsm_state = 1
        elif row[7] == update_t:
            fsm_state = 1
        elif row[7] == downgrade_t:
            fsm_state = 1
        previous_TD_ID = row[0]
    elif fsm_state == 1:
        cursor.execute('UPDATE TRANS_DATA SET ORIGINAL_TD_ID = ? WHERE TD_ID = ?',(row[0],previous_TD_ID))
        fsm_state = 0

#save changes
database.commit()

#Construction of TRANS
h_cursor.execute('SELECT * FROM trans_beg')
for row in h_cursor:
    record_T = [''] * len(TRANS)
    record_T[TRANS.index('T_ID')] = row[0]
    record_T[TRANS.index('beg_timestamp')] = row[1]
    record_T[TRANS.index('RPMDB_version')] = row[2]
    record_T[TRANS.index('loginuid')] = row[3]
    TRANS_INSERT(cursor,record_T)
h_cursor.execute('SELECT * FROM trans_end')
for row in h_cursor:
    cursor.execute('UPDATE TRANS SET end_timestamp=?,return_code=? WHERE T_ID = ?',(row[1],row[3],row[0]))
h_cursor.execute('SELECT * FROM trans_cmdline')
for row in h_cursor:
    cursor.execute('UPDATE TRANS SET cmdline=? WHERE T_ID = ?',(row[1],row[0]))

#fetch releasever
cursor.execute('SELECT T_ID FROM TRANS WHERE releasever=?',('',))
missing = cursor.fetchall()
for row in missing:
    cursor.execute('SELECT PD_ID FROM TRANS_DATA WHERE T_ID=? LIMIT 10',(row[0],))
    PDID = cursor.fetchall()
    if PDID != None:
        for actualPDID in PDID:
            cursor.execute('SELECT P_ID FROM PACKAGE_DATA WHERE PD_ID=? LIMIT 1',(actualPDID[0],))
            actualPID = cursor.fetchone()
            if actualPID != None:
                h_cursor.execute('SELECT yumdb_val FROM pkg_yumdb WHERE pkgtupid=? AND yumdb_key=? LIMIT 1',(actualPID[0],'releasever'))
                releasever = h_cursor.fetchone()
                if releasever != None:
                    cursor.execute('UPDATE TRANS SET releasever=? WHERE T_ID=?',(releasever[0], row[0]))
                    break

#fetch reason
cursor.execute('SELECT TD_ID,PD_ID FROM TRANS_DATA')
missing = cursor.fetchall()
for row in missing:
    cursor.execute('SELECT P_ID FROM PACKAGE_DATA WHERE PD_ID=? LIMIT 1',(row[1],))
    actualPID = cursor.fetchone()
    if actualPID != None:
        h_cursor.execute('SELECT yumdb_val FROM pkg_yumdb WHERE pkgtupid=? AND yumdb_key=? LIMIT 1',(actualPID[0],'reason'))
        reason = h_cursor.fetchone()
        if reason != None:
            t_reason = BIND_REASON(cursor,reason[0])
            cursor.execute('UPDATE TRANS_DATA SET reason=? WHERE TD_ID=?',(t_reason, row[0]))

#contruction of OUTPUT
h_cursor.execute('SELECT * FROM trans_script_stdout')
for row in h_cursor:
    cursor.execute('INSERT INTO OUTPUT VALUES (?,?,?)',(row[1],row[2],BIND_OUTPUT(cursor,'stdout')))
h_cursor.execute('SELECT * FROM trans_error')
for row in h_cursor:
    cursor.execute('INSERT INTO OUTPUT VALUES (?,?,?)',(row[1],row[2],BIND_OUTPUT(cursor,'stderr')))
print('Transforming groups')

#TODO: separate table for packages
#construction of GROUPS
if do_groups == 1:
    with open(groups_path) as groups_file:
        data = json.load(groups_file)
        for key in data:
            if key != 'meta':
                for value in data[key]:
                    record_G = [''] * len(GROUPS)
                    record_G[GROUPS.index('name')] = value
                    record_G[GROUPS.index('exclude')] = str(data[key][value]['pkg_exclude'])
                    record_G[GROUPS.index('pkg_types')] = data[key][value]['pkg_types']
                    record_G[GROUPS.index('grp_types')] = data[key][value]['grp_types']
                    record_G[GROUPS.index('is_installed')] = 1
                    if 'ui_name' in data[key][value]:
                        record_G[GROUPS.index('ui_name')] = data[key][value]['ui_name']
                    for package in data[key][value]['full_list']:
                        if record_G[GROUPS.index('full_list')]:
                            record_G[GROUPS.index('full_list')]+=';'+package
                        else:
                           record_G[GROUPS.index('full_list')]=package
                    cursor.execute('INSERT INTO GROUPS VALUES (null,?,?,?,?,?,?,?)',(record_G))
#TODO: trans group data
#save changes
database.commit()

#close connection
database.close()
historyDB.close()

if task_performed > 0:
    print("Database integrity " + repr(((task_performed - task_failed) / task_performed)*100) + "%")
else:
    print("Database transformation NOT successfull")
