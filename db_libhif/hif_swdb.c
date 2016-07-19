/* hif-swdb.c
*
* Copyright (C) 2016 Red Hat, Inc.
* Author: Eduard Cuba <xcubae00@stud.fit.vutbr.cz>
*
* Licensed under the GNU Lesser General Public License Version 2.1
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/

/* TODO:
 * - Constructor not working
 */

#include "hif_swdb.h"
#include <stdio.h>
#include <stdlib.h>

G_DEFINE_TYPE (HifSwdb, hif_swdb, G_TYPE_OBJECT)

// Class initialiser
static void
hif_swdb_class_init(HifSwdbClass *klass)
{
    GObjectClass *object_class = G_OBJECT_CLASS(klass);
}

// Object initialiser
static void
hif_swdb_init(HifSwdb *self)
{
    self->path = g_strdup("/var/lib/dnf/swdb.sqlite");
    self->ready = 0;
}

//open database at self->path
gint hif_swdb_open(HifSwdb *self)
{
    if(self->ready)
    return 0;

    if( sqlite3_open(hif_swdb_get_path (self), &self->db))
    {
        fprintf(stderr, "ERROR: %s (try again as root)\n", sqlite3_errmsg(self->db));
        return 1;
    }

    self->ready = 1;
    return 0;
}

void hif_swdb_close(HifSwdb *self)
{
    if(self->ready)
    {
        sqlite3_close(self->db);
        self->ready = 0;
    }
}

/*HifSwdb* hif_swdb_new(gchar *path)
{
    HifSwdb *swdb = (HifSwdb *)g_object_new(HIF_TYPE_SWDB, NULL);
  	return swdb;
}*/

// Destructor
void hif_swdb_finalize(HifSwdb *self)
{
    g_object_unref(self);
}

/************************** Functions ********************************/

//returns path to swdb, default is /var/lib/dnf/swdb.sqlite
const gchar   *hif_swdb_get_path  (HifSwdb *self)
{
    return self->path;
}

//changes path to swdb
void  hif_swdb_set_path   (HifSwdb *self, const gchar *path)
{
    if(g_strcmp0(path,self->path) != 0)
    {
        hif_swdb_close(self);
        g_free(self->path);
        self->path = g_strdup(path);
    }
}

//TRUE if db exists else false
gboolean hif_swdb_exist(HifSwdb *self)
{
    return g_file_test(hif_swdb_get_path (self),G_FILE_TEST_EXISTS);
}

gint db_exec(sqlite3 *db, gchar *cmd, int (*callback)(void *, int, char **, char**))
{
    gchar *err_msg;
    gint result = sqlite3_exec(db, cmd, callback, 0, &err_msg);
    if ( result != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    else
    {
        return 0;
    }
}

/* insert into groups/env package tables
 * Returns: 0 if successfull
 * usage: rc = _insert_id_name_desc(db, "TABLE", "Xid", "pkg_name");
 * Requires opened DB
 */
gint _insert_id_name_desc (sqlite3 *db, gchar *table, gint id, gchar *name)
{
    gint rc;
    gchar *err_msg =0;
    sqlite3_stmt *res;
    gchar *sql = g_strjoin(" ","insert into",table,"values (null, @id, @name)");
    rc = sqlite3_prepare_v2(db, sql, -1, &res, NULL);

    if(rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        sqlite3_finalize(res);
        return 2;
    }

    //entity id
    gint idx = sqlite3_bind_parameter_index(res, "@id");
    rc+= sqlite3_bind_int(res, idx, id);

    //package name
    idx = sqlite3_bind_parameter_index(res, "@name");
    rc += sqlite3_bind_text(res, idx, name, -1, SQLITE_STATIC);

    if (rc) //something went wrong
    {
        fprintf(stderr, "SQL error: Could not bind parameters(%s|%d|%s)\n",table,id,name);
        sqlite3_finalize(res);
        return 3;
    }

    if (sqlite3_step(res) != SQLITE_DONE)
    {
        fprintf(stderr, "SQL error: Could not execute statement (insert into %s values(%d,%s)\n",
        table,id,name);
        sqlite3_finalize(res);
        return 4;
    }

    sqlite3_finalize(res);
    return 0;
}

//add new group package
gint hif_swdb_add_group_package (HifSwdb *self, gint gid, gchar *name)
{
    if (hif_swdb_open(self))
    return 1;

    return _insert_id_name_desc (self->db, "GROUPS_PACKAGE", gid, name);
}

//add new group exclude
gint hif_swdb_add_group_exclude (HifSwdb *self, gint gid, gchar *name)
{
    if (hif_swdb_open(self))
    return 1;

    return _insert_id_name_desc (self->db, "GROUPS_EXCLUDE", gid, name);
}


/* Bind description to id in chosen table
 * Returns: ID for desctiption (adds new element if description not present)
 * Usage: _bind_desc_id(db, table, description)
 * Requires opened DB
 */
gint _bind_desc_id(sqlite3 *db, gchar *table, gchar *desc)
{
    gint rc;
    gchar *err_msg = 0;
    sqlite3_stmt *res;
    gchar *sql = g_strjoin(" ","select ID from",table,"where description=@desc");
    rc = sqlite3_prepare_v2(db, sql, -1, &res, NULL);
    if(rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        sqlite3_finalize(res);
        return -2;
    }

    gint idx = sqlite3_bind_parameter_index(res, "@desc");
    rc += sqlite3_bind_text(res, idx, desc, -1, SQLITE_STATIC);

    if (rc) //something went wrong
    {
        fprintf(stderr, "SQL error: Could not bind parameters(%s|%s)\n",table,desc);
        sqlite3_finalize(res);
        return -3;
    }

    if (sqlite3_step(res) == SQLITE_ROW ) // id for description found
    {
        gint result = sqlite3_column_int(res, 0);
        sqlite3_finalize(res);
        return result;
    }
    else // id for desc not found, try to add it
    {
        sqlite3_finalize(res);
        gchar *sql_add = g_strjoin(" ","insert into",table,"values (null, @desc)");
        rc = sqlite3_prepare_v2(db, sql_add, -1, &res, NULL);
        if(rc != SQLITE_OK)
        {
            fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
            sqlite3_free(err_msg);
            sqlite3_finalize(res);
            return -2;
        }

        idx = sqlite3_bind_parameter_index(res, "@desc");
        rc += sqlite3_bind_text(res, idx, desc, -1, SQLITE_STATIC);

        if (rc) //something went wrong
        {
            fprintf(stderr, "SQL error: Could not bind parameters(%s|%s)\n",table,desc);
            sqlite3_finalize(res);
            return -3;
        }
        gint step = sqlite3_step(res); //add desc into table

        if (step != SQLITE_DONE) //not added
        {
            fprintf(stderr, "SQL error: Could not execute statement (insert into %s value %s)\n",
            table,desc);
            sqlite3_finalize(res);
            return -4;
        }

        if ( step == SQLITE_ROW ) //added
        {
            gint result = sqlite3_column_int(res, 0);
            sqlite3_finalize(res);
            return result;
        }
        else //some problem
        {
            sqlite3_finalize(res);
            return -1;
        }
    }
}


/* Binder for PACKAGE_TYPE
 * Returns: ID of description in PACKAGE_TYPE table
 * Usage: hif_swdb_get_package_type( HifSwdb, description)
 */
gint hif_swdb_get_package_type (HifSwdb *self, gchar *type)
{
 	if(hif_swdb_open(self))
 		return -1;
  	return _bind_desc_id(self->db, "PACKAGE_TYPE", type);
}

/* OUTPUT_TYPE binder
 * Returns: ID of description in OUTPUT_TYPE table
 * Usage: hif_swdb_get_output_type( HifSwdb, description)
 */
gint hif_swdb_get_output_type (HifSwdb *self, gchar *type)
{
	if(hif_swdb_open(self))
 		return -1;
  	return _bind_desc_id(self->db, "OUTPUT_TYPE", type);
}

/* REASON_TYPE binder
 * Returns: ID of description in REASON_TYPE table
 * Usage: hif_swdb_get_reason_type( HifSwdb, description)
 */
gint hif_swdb_get_reason_type (HifSwdb *self, gchar *type)
{
	if(hif_swdb_open(self))
 		return -1;
  	return _bind_desc_id(self->db, "REASON_TYPE", type);
}

/* STATE_TYPE binder
 * Returns: ID of description in STATE_TYPE table
 * Usage: hif_swdb_get_state_type( HifSwdb, description)
 */
gint hif_swdb_get_state_type (HifSwdb *self, gchar *type)
{
	if(hif_swdb_open(self))
 		return -1;
  	return _bind_desc_id(self->db, "STATE_TYPE", type);
}

//create db at path
gint hif_swdb_create_db (HifSwdb *self)
{
    if (hif_swdb_open(self))
    	return 1;

    // Create all tables
    gint failed = 0;

    //PACKAGE_DATA
    failed += db_exec (self->db," CREATE TABLE PACKAGE_DATA ( PD_ID integer PRIMARY KEY,\
                                    P_ID integer, R_ID integer, from_repo_revision text,\
                                    from_repo_timestamp text, installed_by text, changed_by text,\
                                    installonly text, origin_url text)", NULL);
    //PACKAGE
    failed += db_exec (self->db, "CREATE TABLE PACKAGE ( P_ID integer, name text, epoch text,\
                                    version text, release text, arch text, checksum_data text,\
                                    checksum_type text, type integer )", NULL);
    //REPO
    failed += db_exec (self->db, "CREATE TABLE REPO (R_ID INTEGER PRIMARY KEY, name text,\
                                    last_synced text, is_expired text)", NULL);
    //TRANS_DATA
    failed += db_exec (self->db, "CREATE TABLE TRANS_DATA (TD_ID INTEGER PRIMARY KEY,\
                                    T_ID integer,PD_ID integer, G_ID integer, done INTEGER,\
                                    ORIGINAL_TD_ID integer, reason integer, state integer)", NULL);
    //TRANS
    failed += db_exec (self->db," CREATE TABLE TRANS (T_ID integer, beg_timestamp text, \
                                    end_timestamp text, RPMDB_version text, cmdline text, \
                                    loginuid integer, releasever text, return_code integer) ", NULL);
    //OUTPUT
    failed += db_exec (self->db, "CREATE TABLE OUTPUT (T_ID INTEGER, msg text, type integer)", NULL);

    //STATE_TYPE
    failed += db_exec (self->db, "CREATE TABLE STATE_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //REASON_TYPE
    failed += db_exec (self->db, "CREATE TABLE REASON_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //OUTPUT_TYPE
    failed += db_exec (self->db, "CREATE TABLE OUTPUT_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //PACKAGE_TYPE
    failed += db_exec (self->db, "CREATE TABLE PACKAGE_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //GROUPS
    failed += db_exec (self->db, "CREATE TABLE GROUPS (G_ID INTEGER PRIMARY KEY, name_id text, name text,\
                                    ui_name text, is_installed integer, pkg_types integer, grp_types integer)", NULL);
    //TRANS_GROUP_DATA
    failed += db_exec (self->db, "CREATE TABLE TRANS_GROUP_DATA (TG_ID INTEGER PRIMARY KEY, T_ID integer,\
                                    G_ID integer, name_id text, name text, ui_name text,\
                                    is_installed integer, pkg_types integer, grp_types integer)", NULL);
    //GROUPS_PACKAGE
    failed += db_exec (self->db, "CREATE TABLE GROUPS_PACKAGE (GP_ID INTEGER PRIMARY KEY,\
                                    G_ID integer, name text)", NULL);
    //GROUPS_EXCLUDE
    failed += db_exec (self->db, "CREATE TABLE GROUPS_EXCLUDE (GE_ID INTEGER PRIMARY KEY,\
                                    G_ID integer, name text)", NULL);
    //ENVIRONMENTS_GROUPS
    failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS_GROUPS (EG_ID INTEGER PRIMARY KEY,\
                                    E_ID integer, G_ID integer)", NULL);
    //ENVIRONMENTS
    failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS (E_ID INTEGER PRIMARY KEY, name_id text,\
                                    name text, ui_name text, pkg_types integer, grp_types integer)", NULL);
    //ENVIRONMENTS_EXCLUDE
    failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS_EXCLUDE (EE_ID INTEGER PRIMARY KEY,\
                                    E_ID integer, name text)", NULL);

    if (failed != 0)
    {
        fprintf(stderr, "SQL error: Unable to create %d tables\n",failed);
        sqlite3_close(self->db);
        return 2;
    }

    hif_swdb_close(self);
    return 0;
}

//remove and init empty db
gint hif_swdb_reset_db (HifSwdb *self)
{
    if(hif_swdb_exist(self))
    {
        hif_swdb_close(self);
        if (g_remove(self->path)!= 0)
        {
            fprintf(stderr,"SWDB error: Could not reset database (try again as root)\n");
            return 1;
        }
    }
    return hif_swdb_create_db(self);
}
