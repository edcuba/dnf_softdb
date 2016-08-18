#!/usr/bin/bash
# After this you should be able to call libhif_swdb functions from python
# You need glib, gobject and ipyhon installed
# Even if you done this right it may not work...
# Source: http://blog-vpodzime.rhcloud.com/?p=33#id3
# Eduard Cuba 2016
set -e
gcc -c -Wmissing-declarations -Wreturn-type -Wunused-variable -Wdiscarded-qualifiers -o hif-swdb.o -fPIC `pkg-config --cflags --libs gobject-2.0 sqlite3` hif-swdb.c
gcc -shared -o libhif-swdb.so hif-swdb.o `pkg-config --libs gobject-2.0 sqlite3`
LD_LIBRARY_PATH=. g-ir-scanner --no-libtool `pkg-config --libs --cflags gobject-introspection-1.0 gobject-2.0` --include=GObject-2.0 --identifier-prefix=Hif --symbol-prefix=hif_ --namespace Hif --nsversion=1.0 --library hif-swdb --warn-all -o Hif-1.0.gir hif-swdb.c hif-swdb.h hif-swdb-obj.h
g-ir-compiler Hif-1.0.gir > Hif-1.0.typelib
#export GI_TYPELIB_PATH=`pwd`
#export LD_LIBRARY_PATH=`pwd`
#ipython
cp hif-swdb.c /home/edynox/devel/libhif/libhif/
cp hif-swdb.h /home/edynox/devel/libhif/libhif/
cp hif-swdb-sql.h /home/edynox/devel/libhif/libhif/
cp hif-swdb-obj.h /home/edynox/devel/libhif/libhif/
cd '/home/edynox/devel/libhif/build/'
cmake ..
make
sudo make install
