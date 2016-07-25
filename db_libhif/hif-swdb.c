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

#define default_path "/var/lib/dnf/swdb.sqlite"

#define DB_PREP(db, sql, res) assert(_db_prepare(db, sql, &res))
#define DB_BIND(res, id, source) assert(_db_bind(res, id, source))
#define DB_BIND_INT(res, id, source) assert(_db_bind_int(res, id, source))
#define DB_STEP(res) assert(_db_step(res))

#define INSERT_PKG "insert into PACKAGE values(null,@name,@epoch,@version,@release,@arch,@cdata,@ctype,@type)"

#include "hif-swdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

struct _HifSwdb
{
    GObject parent_instance;
    gchar   *path;
    sqlite3 *db;
    gboolean ready; //db opened
  	gboolean path_changed; //dont forget to free memory...
  	gboolean running; //if true, db will not be closed
};

G_DEFINE_TYPE(HifSwdb, hif_swdb, G_TYPE_OBJECT)

/* Table structs */
struct package_t
{
  	const gchar *name;
  	const gchar *epoch;
  	const gchar *version;
  	const gchar *release;
  	const gchar *arch;
  	const gchar *checksum_data;
  	const gchar *checksum_type;
  	gint type;
};


// Destructor
static void hif_swdb_finalize(GObject *object)
{
    G_OBJECT_CLASS (hif_swdb_parent_class)->finalize (object);
}

// Class initialiser
static void
hif_swdb_class_init(HifSwdbClass *klass)
{
    GObjectClass *object_class = G_OBJECT_CLASS(klass);
    object_class->finalize = hif_swdb_finalize;

}

// Object initialiser
static void
hif_swdb_init(HifSwdb *self)
{
  	self->path = (gchar*) default_path;
	self->ready = 0;
  	self->path_changed = 0;
  	self->running = 0;
}

/**
 * hif_swdb_new:
 *
 * Creates a new #HifSwdb.
 *
 * Returns: a #HifSwdb
 **/
HifSwdb* hif_swdb_new(void)
{
    HifSwdb *swdb = g_object_new(HIF_TYPE_SWDB, NULL);
  	return swdb;
}

/******************************* Functions *************************************/

static gint _db_step(sqlite3_stmt *res)
{
  	if (sqlite3_step(res) != SQLITE_DONE)
    {
        fprintf(stderr, "SQL error: Could not execute statement in _db_step()\n");
        sqlite3_finalize(res);
        return 0;
	}
  	sqlite3_finalize(res);
  	return 1; //true because of assert
}

static gint _db_prepare(sqlite3 *db, const gchar *sql, sqlite3_stmt **res)
{
  	gint rc = sqlite3_prepare_v2(db, sql, -1, res, NULL);
    if(rc != SQLITE_OK)
    {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
	  	fprintf(stderr, "SQL error: Prepare failed!\n");
        sqlite3_finalize(*res);
        return 0;
    }
  	return 1; //true because of assert
}

static gint _db_bind(sqlite3_stmt *res, const gchar *id, const gchar *source)
{
  	gint idx = sqlite3_bind_parameter_index(res, id);
    gint rc = sqlite3_bind_text(res, idx, source, -1, SQLITE_STATIC);

  	if (rc) //something went wrong
    {
        fprintf(stderr, "SQL error: Could not bind parameters(%d|%s|%s)\n",idx,id,source);
        sqlite3_finalize(res);
        return 0;
    }
  	return 1; //true because of assert
}

static gint _db_bind_int(sqlite3_stmt *res, const gchar *id, gint source)
{
  	gint idx = sqlite3_bind_parameter_index(res, id);
    gint rc = sqlite3_bind_int(res, idx, source);

  	if (rc) //something went wrong
    {
        fprintf(stderr, "SQL error: Could not bind parameters(%s|%d)\n",id,source);
        sqlite3_finalize(res);
        return 0;
    }
  	return 1; //true because of assert
}

static gint _db_exec(sqlite3 *db, const gchar *cmd, int (*callback)(void *, int, char **, char**))
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

/**
 * _package_insert: (skip)
 * Insert package into database
 * @db - database
 * @package - package meta struct
 **/
static gint _package_insert(sqlite3 *db, struct package_t *package)
{
    sqlite3_stmt *res;
   	const gchar *sql = INSERT_PKG;
	DB_PREP(db,sql,res);
  	DB_BIND(res, "@name", package->name);
  	DB_BIND(res, "@epoch", package->epoch);
  	DB_BIND(res, "@version", package->version);
  	DB_BIND(res, "@release", package->release);
  	DB_BIND(res, "@arch", package->arch);
  	DB_BIND(res, "@cdata", package->checksum_data);
  	DB_BIND(res, "@ctype", package->checksum_type);
  	DB_BIND_INT(res, "@type", package->type);
	DB_STEP(res);
  	return 0;
}


/******************************* GROUP PERSISTOR *******************************/

/* insert into groups/env package tables
 * Returns: 0 if successfull
 * usage: rc = _insert_id_name(db, "TABLE", "Xid", "pkg_name");
 * Requires opened DB
 */
static gint _insert_id_name (sqlite3 *db, const gchar *table, gint id, const gchar *name)
{
    sqlite3_stmt *res;
    gchar *sql = g_strjoin(" ","insert into",table,"values (null, @id, @name)", NULL);

    DB_PREP(db,sql,res);

    //entity id
	DB_BIND_INT(res, "@id", id);

    //package name
    DB_BIND(res, "@name", name);

    DB_STEP(res);
  	return 0;
}

//add new group package
gint hif_swdb_add_group_package (HifSwdb *self, gint gid, const gchar *name)
{
    if (hif_swdb_open(self))
    return 1;

    gint rc = _insert_id_name (self->db, "GROUPS_PACKAGE", gid, name);
	hif_swdb_close(self);
  	return rc;
}

//add new group exclude
gint hif_swdb_add_group_exclude (HifSwdb *self, gint gid, const gchar *name)
{

    if (hif_swdb_open(self))
    return 1;

    gint rc = _insert_id_name (self->db, "GROUPS_EXCLUDE", gid, name);
	hif_swdb_close(self);
  	return rc;
}

//add new environments exclude
gint hif_swdb_add_environments_exclude (HifSwdb *self, gint eid, const gchar *name)
{

    if (hif_swdb_open(self))
    return 1;

    gint rc = _insert_id_name (self->db, "ENVIRONMENTS_EXCLUDE", eid, name);
	hif_swdb_close(self);
  	return rc;
}



/****************************** PACKAGE PERSISTOR ****************************/

gint hif_swdb_add_package_naevrcht(	HifSwdb *self,
				  					const gchar *name,
				  					const gchar *arch,
				  					const gchar *epoch,
				  					const gchar *version,
				  					const gchar *release,
				 					const gchar *checksum_data,
								   	const gchar *checksum_type,
								  	const gchar *type)
{
  	if (hif_swdb_open(self))
    	return 1;
  	self->running = 1;

  	//transform data into nevra format
 	struct package_t package = {name, epoch, version, release, arch,
		checksum_data, checksum_type, hif_swdb_get_package_type(self,type)};

  	gint rc = _package_insert(self->db, &package);
  	self->running = 0;
  	hif_swdb_close(self);
  	return rc;
}




/****************************** TYPE BINDERS *********************************/

/* Finds ID by description in @table
 * Returns ID for description or 0 if not found
 * Requires opened DB
 */
static gint _find_match_by_desc(sqlite3 *db, const gchar *table, const gchar *desc)
{
    sqlite3_stmt *res;
    const gchar *sql = g_strjoin(" ","select ID from",table,"where description=@desc", NULL);

    DB_PREP(db,sql,res);
    DB_BIND(res, "@desc", desc);
    if (sqlite3_step(res) == SQLITE_ROW ) // id for description found
    {
        gint result = sqlite3_column_int(res, 0);
        sqlite3_finalize(res);
        return result;
    }
  	else
	{
	  sqlite3_finalize(res);
	  return 0;
	}
}

/* Inserts description into @table
 * Returns 0 if successfull
 * Requires opened DB
 */
static gint _insert_desc(sqlite3 *db, const gchar *table, const gchar *desc)
{
    sqlite3_stmt *res;
    gchar *sql = g_strjoin(" ","insert into",table,"values (null, @desc)", NULL);

  	DB_PREP(db, sql, res);
    DB_BIND(res, "@desc", desc);
	DB_STEP(res);
  	return 0;
}

/* Bind description to id in chosen table
 * Returns: ID of desctiption (adds new element if description not present), <= 0 if error
 * Usage: _bind_desc_id(db, table, description)
 * Requires opened DB
 */
static gint _bind_desc_id(sqlite3 *db, const gchar *table, const gchar *desc)
{
    gint id = _find_match_by_desc(db,table,desc);
  	if(id) //found or error
	{
	  	return id;
	}
    else // id for desc not found, try to add it
    {
        id = _insert_desc(db,table,desc);
	  	if(id) //error
		{
		  return id;
		}
	  	return _find_match_by_desc(db,table,desc);
    }
}


/* Binder for PACKAGE_TYPE
 * Returns: ID of description in PACKAGE_TYPE table
 * Usage: hif_swdb_get_package_type( HifSwdb, description)
 */
gint hif_swdb_get_package_type (HifSwdb *self, const gchar *type)
{

 	if(hif_swdb_open(self))
 		return -1;
  	gint rc = _bind_desc_id(self->db, "PACKAGE_TYPE", type);
	hif_swdb_close(self);
  	return rc;
}

/* OUTPUT_TYPE binder
 * Returns: ID of description in OUTPUT_TYPE table
 * Usage: hif_swdb_get_output_type( HifSwdb, description)
 */
gint hif_swdb_get_output_type (HifSwdb *self, const gchar *type)
{
	if(hif_swdb_open(self))
 		return -1;
  	gint rc = _bind_desc_id(self->db, "OUTPUT_TYPE", type);
  	hif_swdb_close(self);
  	return rc;
}

/* REASON_TYPE binder
 * Returns: ID of description in REASON_TYPE table
 * Usage: hif_swdb_get_reason_type( HifSwdb, description)
 */
gint hif_swdb_get_reason_type (HifSwdb *self, const gchar *type)
{

	if(hif_swdb_open(self))
 		return -1;
  	gint rc = _bind_desc_id(self->db, "REASON_TYPE", type);
  	hif_swdb_close(self);
  	return rc;
}

/* STATE_TYPE binder
 * Returns: ID of description in STATE_TYPE table
 * Usage: hif_swdb_get_state_type( HifSwdb, description)
 */
gint hif_swdb_get_state_type (HifSwdb *self, const gchar *type)
{

	if(hif_swdb_open(self))
 		return -1;
  	gint rc = _bind_desc_id(self->db, "STATE_TYPE", type);
  	hif_swdb_close(self);
  	return rc;
}

/******************************* Database operators ************************************/

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
	    if (self->path_changed)
		{
		  g_free(self->path);
		}
	  	self->path_changed = 1;
		self->path = g_strdup(path);
    }
}

//TRUE if db exists else false
gboolean hif_swdb_exist(HifSwdb *self)
{
    return g_file_test(hif_swdb_get_path (self),G_FILE_TEST_EXISTS);
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
    if( self->ready && !self->running )
    {
        sqlite3_close(self->db);
        self->ready = 0;
    }
}

//create db at path
gint hif_swdb_create_db (HifSwdb *self)
{

    if (hif_swdb_open(self))
    	return 1;

    // Create all tables
    gint failed = 0;

    //PACKAGE_DATA
    failed += _db_exec (self->db," CREATE TABLE PACKAGE_DATA ( PD_ID integer PRIMARY KEY,"
                                    "P_ID integer, R_ID integer, from_repo_revision text,"
                                    "from_repo_timestamp text, installed_by text, changed_by text,"
                                    "installonly text, origin_url text)", NULL);
    //PACKAGE
    failed += _db_exec (self->db, "CREATE TABLE PACKAGE ( P_ID integer primary key, name text, epoch text,"
                                    "version text, release text, arch text, checksum_data text,"
                                    "checksum_type text, type integer )", NULL);
    //REPO
    failed += _db_exec (self->db, "CREATE TABLE REPO (R_ID INTEGER PRIMARY KEY, name text,"
                                    "last_synced text, is_expired text)", NULL);
    //TRANS_DATA
    failed += _db_exec (self->db, "CREATE TABLE TRANS_DATA (TD_ID INTEGER PRIMARY KEY,"
                                    "T_ID integer,PD_ID integer, G_ID integer, done INTEGER,"
                                    "ORIGINAL_TD_ID integer, reason integer, state integer)", NULL);
    //TRANS
    failed += _db_exec (self->db," CREATE TABLE TRANS (T_ID integer primary key, beg_timestamp text,"
                                    "end_timestamp text, RPMDB_version text, cmdline text,"
                                    "loginuid integer, releasever text, return_code integer) ", NULL);
    //OUTPUT
    failed += _db_exec (self->db, "CREATE TABLE OUTPUT (O_ID integer primary key,T_ID INTEGER, msg text, type integer)", NULL);

    //STATE_TYPE
    failed += _db_exec (self->db, "CREATE TABLE STATE_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //REASON_TYPE
    failed += _db_exec (self->db, "CREATE TABLE REASON_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //OUTPUT_TYPE
    failed += _db_exec (self->db, "CREATE TABLE OUTPUT_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //PACKAGE_TYPE
    failed += _db_exec (self->db, "CREATE TABLE PACKAGE_TYPE (ID INTEGER PRIMARY KEY, description text)", NULL);

    //GROUPS
    failed += _db_exec (self->db, "CREATE TABLE GROUPS (G_ID INTEGER PRIMARY KEY, name_id text, name text,"
                                    "ui_name text, is_installed integer, pkg_types integer, grp_types integer)", NULL);
    //TRANS_GROUP_DATA
    failed += _db_exec (self->db, "CREATE TABLE TRANS_GROUP_DATA (TG_ID INTEGER PRIMARY KEY, T_ID integer,"
                                    "G_ID integer, name_id text, name text, ui_name text,"
                                    "is_installed integer, pkg_types integer, grp_types integer)", NULL);
    //GROUPS_PACKAGE
    failed += _db_exec (self->db, "CREATE TABLE GROUPS_PACKAGE (GP_ID INTEGER PRIMARY KEY,"
                                    "G_ID integer, name text)", NULL);
    //GROUPS_EXCLUDE
    failed += _db_exec (self->db, "CREATE TABLE GROUPS_EXCLUDE (GE_ID INTEGER PRIMARY KEY,"
                                    "G_ID integer, name text)", NULL);
    //ENVIRONMENTS_GROUPS
    failed += _db_exec (self->db, "CREATE TABLE ENVIRONMENTS_GROUPS (EG_ID INTEGER PRIMARY KEY,"
                                    "E_ID integer, G_ID integer)", NULL);
    //ENVIRONMENTS
    failed += _db_exec (self->db, "CREATE TABLE ENVIRONMENTS (E_ID INTEGER PRIMARY KEY, name_id text,"
                                    "name text, ui_name text, pkg_types integer, grp_types integer)", NULL);
    //ENVIRONMENTS_EXCLUDE
    failed += _db_exec (self->db, "CREATE TABLE ENVIRONMENTS_EXCLUDE (EE_ID INTEGER PRIMARY KEY,"
                                    "E_ID integer, name text)", NULL);

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
