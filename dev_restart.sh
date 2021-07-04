#!/bin/bash
#Quick restart script for dev use

SCIDB_INSTALL=/opt/scidb/19.11
$SCIDB_INSTALL/bin/iquery -aq "unload_library('array_hash')" > /dev/null 2>&1
set -e
mydir=`dirname $0`
pushd $mydir
make clean
make SCIDB=$SCIDB_INSTALL
sudo cp libarray_hash.so $SCIDB_INSTALL/lib/scidb/plugins/
sudo systemctl restart scidb
sleep 20
#for multinode setups, dont forget to copy to every instance
iquery -aq "load_library('array_hash')"

