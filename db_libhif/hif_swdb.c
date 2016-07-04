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

//create db at path
gint hif_swdb_create_db (HifSwdb *self)
{
  gint rc = sqlite3_open(hif_swdb_get_path (self), &self->db);
  if ( rc )
    {
      fprintf(stderr, "ERROR: %s (try again as root)\n", sqlite3_errmsg(self->db));
      return -1;
    }

  gchar *sql;
  gchar *zErrMsg = 0;

  /* Create all tables */
   sql = "CREATE TABLE...";
   rc = sqlite3_exec(self->db, sql, NULL, 0, &zErrMsg);

  if( rc != SQLITE_OK )
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }

  sqlite3_close(self->db);
  return 0;
}


