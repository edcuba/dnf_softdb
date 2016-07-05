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

#include "hif_swdb.h"
#include <stdio.h>
#include <sqlite3.h>

typedef struct _HifSwdb HifSwdb;

/* Default structure */
struct _HifSwdb
{
    GObject parent_instance;
    gchar   *path;
    sqlite3 *db;
};

G_DEFINE_TYPE (HifSwdb, hif_swdb, G_TYPE_OBJECT)

//property enum
enum
{
  PROP_0,
  PROP_PATH,
  LAST_PROP,
};

//optimalisation
static GParamSpec *properties[LAST_PROP];

//property operands
static void
hif_swdb_get_property (GObject *object, guint prop_id, GValue *value, GParamSpec *pspec)
{
  HifSwdb *self = (HifSwdb*)object;
  switch (prop_id)
  {
    case PROP_PATH:
      g_value_set_string (value, hif_swdb_get_path(self));
      break;
  }
}

static void
hif_swdb_set_property (GObject *object, guint prop_id, const GValue *value, GParamSpec *pspec)
{
  HifSwdb *self = (HifSwdb*)object;
  switch (prop_id)
  {
    case PROP_PATH:
      hif_swdb_set_path (self, g_value_get_string(value));
      break;
  }
}


// Class initialiser
static void
hif_swdb_class_init(HifSwdbClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS(klass);
  object_class->get_property = hif_swdb_get_property;
  object_class->set_property = hif_swdb_set_property;

  properties[PROP_PATH] = g_param_spec_string ( "path", "Path", "Path to swdb", NULL,
                        (G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_properties(object_class, LAST_PROP, properties);
}

// Object initialiser
static void
hif_swdb_init(HifSwdb *self)
{
  self->path = g_strdup("/var/lib/dnf/swdb.sqlite");
}

// Constructor
HifSwdb* hif_swdb_new(void)
{
     return g_object_new(HIF_TYPE_SWDB, 0);
}

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
    g_free(self->path);
    self->path = g_strdup(path);
  }
}

//TRUE if db exists else false
gboolean hif_swdb_exist(HifSwdb *self)
{
  return g_file_test(hif_swdb_get_path (self),G_FILE_TEST_EXISTS);
}

gint db_exec(sqlite3 *db, gchar *cmd)
{
  gchar *err_msg;
  gint result = sqlite3_exec(db, cmd, 0, 0, &err_msg);
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

//create db at path
gint hif_swdb_create_db (HifSwdb *self)
{
  gint rc = sqlite3_open(hif_swdb_get_path (self), &self->db);
  if ( rc )
    {
      fprintf(stderr, "ERROR: %s (try again as root)\n", sqlite3_errmsg(self->db));
      return 1;
    }

  /* Create all tables */
  gint failed = 0;

  //PACKAGE_DATA
  failed += db_exec (self->db, "CREATE TABLE PACKAGE_DATA ( PD_ID integer PRIMARY KEY,\
                                P_ID integer, R_ID integer, from_repo_revision text,\
                                from_repo_timestamp text, installed_by text, changed_by text,\
                                installonly text, origin_url text)");
  //PACKAGE
  failed += db_exec (self->db, "CREATE TABLE PACKAGE ( P_ID integer, name text, epoch text,\
                                version text, release text, arch text, checksum_data text,\
                                checksum_type text, type integer )");
  //REPO
  failed += db_exec (self->db, "CREATE TABLE REPO (R_ID INTEGER PRIMARY KEY, name text,\
                                last_synced text, is_expired text)");
  //TRANS_DATA
  failed += db_exec (self->db, "CREATE TABLE TRANS_DATA (TD_ID INTEGER PRIMARY KEY,\
                                T_ID integer,PD_ID integer, G_ID integer, done INTEGER,\
                                ORIGINAL_TD_ID integer, reason integer, state integer)");
  //TRANS
  failed += db_exec (self->db, "CREATE TABLE TRANS (T_ID integer, beg_timestamp text,\
                                end_timestamp text, RPMDB_version text, cmdline text,\
                                loginuid integer, releasever text, return_code integer)");
  //OUTPUT
  failed += db_exec (self->db, "CREATE TABLE OUTPUT (T_ID INTEGER, msg text, type integer)");

  //STATE_TYPE
  failed += db_exec (self->db, "CREATE TABLE STATE_TYPE (ID INTEGER PRIMARY KEY, description text)");

  //REASON_TYPE
  failed += db_exec (self->db, "CREATE TABLE REASON_TYPE (ID INTEGER PRIMARY KEY, description text)");

  //OUTPUT_TYPE
  failed += db_exec (self->db, "CREATE TABLE OUTPUT_TYPE (ID INTEGER PRIMARY KEY, description text)");

  //PACKAGE_TYPE
  failed += db_exec (self->db, "CREATE TABLE PACKAGE_TYPE (ID INTEGER PRIMARY KEY, description text)");

  //GROUPS
  failed += db_exec (self->db, "CREATE TABLE GROUPS (G_ID INTEGER PRIMARY KEY, name_id text, name text,\
                                ui_name text, is_installed integer, pkg_types integer, grp_types integer)");
  //TRANS_GROUP_DATA
  failed += db_exec (self->db, "CREATE TABLE TRANS_GROUP_DATA (TG_ID INTEGER PRIMARY KEY, T_ID integer,\
                                G_ID integer, name_id text, name text, ui_name text,\
                                is_installed integer, pkg_types integer, grp_types integer)");
  //GROUPS_PACKAGE
  failed += db_exec (self->db, "CREATE TABLE GROUPS_PACKAGE (GP_ID INTEGER PRIMARY KEY,\
                                G_ID integer, name text)");
  //GROUPS_EXCLUDE
  failed += db_exec (self->db, "CREATE TABLE GROUPS_EXCLUDE (GE_ID INTEGER PRIMARY KEY,\
                                G_ID integer, name text)");
  //ENVIRONMENTS_GROUPS
  failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS_GROUPS (EG_ID INTEGER PRIMARY KEY,\
                                E_ID integer, G_ID integer)");
  //ENVIRONMENTS
  failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS (E_ID INTEGER PRIMARY KEY, name_id text,\
                                name text, ui_name text, pkg_types integer, grp_types integer)");
  //ENVIRONMENTS_EXCLUDE
  failed += db_exec (self->db, "CREATE TABLE ENVIRONMENTS_EXCLUDE (EE_ID INTEGER PRIMARY KEY,\
                                E_ID integer, name text)");
  if (failed != 0)
    {
      fprintf(stderr, "SQL error: Unable to create %d tables\n",failed);
      sqlite3_close(self->db);
      return 2;
    }

  sqlite3_close(self->db);
  return 0;
}

//remove and init empty db
gint hif_swdb_reset_db (HifSwdb *self)
{
  if(hif_swdb_exist(self))
    {
  	if (g_remove(self->path)!= 0)
	{
	  fprintf(stderr,"SWDB error: Could not reset database (try again as root)\n");
	  return 1;
	}
    }
  return hif_swdb_create_db(self);
}

