/* hif-swdb.h
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

#ifndef HIF_SWDB_H
#define HIF_SWDB_H

#include <glib-object.h>
#include <glib.h>
#include <glib/gstdio.h>

G_BEGIN_DECLS

#define HIF_TYPE_SWDB (hif_swdb_get_type ())

G_DECLARE_FINAL_TYPE (HifSwdb, hif_swdb, HIF,SWDB, GObject) // structure,function prefix,namespace,object name,inherits

/* returns path to swdb */
const gchar *hif_swdb_get_path (HifSwdb *self);

/* change path to swdb - actual swdb is closed first */
void  hif_swdb_set_path (HifSwdb *self, const gchar *path);

/* Constructor */
HifSwdb* hif_swdb_new(void);

/* Destructor */
void hif_swdb_finalize(HifSwdb *self);

/* True when swdb exist */
gboolean hif_swdb_exist(HifSwdb *self);

/* Create new swdb */
gint hif_swdb_create_db (HifSwdb *self);

/* Remove old and create new */
gint hif_swdb_reset_db (HifSwdb *self);

/* Bind description with id in table PACKAGE_TYPE */
gint hif_swdb_get_package_type (HifSwdb *self, gchar *state);

/* Add package name of group gid into table GROUPS_PACKAGE */
gint hif_swdb_add_group_package (HifSwdb *self, gint gid, gchar *name);

/* Add package name of group gid into table GROUPS_EXCLUDE */
gint hif_swdb_add_group_exclude (HifSwdb *self, gint gid, gchar *name);

gint hif_sw


G_END_DECLS

#endif
