#!/usr/bin/bash
# After this you should be able to call libhif_swdb functions from python
# You need glib, gobject and ipyhon installed
# Even if you done this right it may not work...
# Source: http://blog-vpodzime.rhcloud.com/?p=33#id3
# Eduard Cuba 2016
set -e
gcc -c -o hif_swdb.o -fPIC `pkg-config --cflags glib-2.0 gobject-2.0 sqlite3` hif_swdb.c
gcc -shared -o libhif_swdb.so hif_swdb.o
LD_LIBRARY_PATH=. g-ir-scanner `pkg-config --libs --cflags glib-2.0 gobject-2.0 sqlite3` --identifier-prefix=Hif --symbol-prefix=hif_swdb --namespace HifSwdb --nsversion=1.0 --library hif_swdb --warn-all -o HifSwdb-1.0.gir hif_swdb.c hif_swdb.h
g-ir-compiler HifSwdb-1.0.gir > HifSwdb-1.0.typelib
GI_TYPELIB_PATH=. LD_LIBRARY_PATH=. ipython
