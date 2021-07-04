#!/bin/bash

MYDIR=$(cd $(dirname $0) && pwd)

# --outfile <filename> : write iquery output to the given file.
# This is important for running in the SciDB test harness.
if [ "$1" = "--outfile" ]; then
    OUTFILE="$2"
    shift 2
else
    OUTFILE=$MYDIR/test.out
fi

rm -f "$OUTFILE"

iquery -anq "remove(foo)" > /dev/null 2>&1

iquery -anq "
store(
 apply(
  build(<val: double null> [i=1:4000000,500000,0], iif( i % 10=0, null, i % 100 )),
  a, iif(i % 2 =  0, 'abc', 'def'),
  b, iif(i % 5 =  0, null, string(i) + '0'),
  c, iif(i % 10 = 0, null, iif(i % 9=0, double(nan), i % 20 ))
 ),
 foo
)"

echo "These should match" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(foo)" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(_sg(foo, 3))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(_sg(foo, 4))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(flatten(foo))" >> $OUTFILE
#TODO: why does this assert without an SG? Optimizer returns wrong distribution type
iquery -ocsv:l -aq "array_hash(_sg(redimension(foo, <val:double,a:string NOT NULL,b:string,c:double> [i=1:4000000:0:10000] ), 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(filter(foo, i>0))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(cast(foo, <val_renamed:double,a_renamed:string NOT NULL,b_renamed:string,c_renamed:double>[i=1:4000000:0:500000]))" >> $OUTFILE

echo "These should diverge" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(filter(foo, i>1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(filter(foo, i<4000000))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(project(foo, b,c,a,val))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(filter(foo, a='abc'))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(apply(foo, z, double(null)))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(cast(foo, <val:float>))" >> $OUTFILE

echo "These should also diverge" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:string>[i=1:1], 'Hello, World'))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:string>[i=1:1], 'Hello World'))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:string>[i=1:1], null))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:double>[i=1:1], 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:int64>[i=1:1], 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:int32>[i=1:1], 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:uint16>[i=1:1], 1))" >> $OUTFILE

echo "But these actually match" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:int8>[i=1:1], 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:uint8>[i=1:1], 1))" >> $OUTFILE
iquery -ocsv:l -aq "array_hash(build(<val:bool>[i=1:1], 1))" >> $OUTFILE

diff $OUTFILE $MYDIR/test.expected && echo "All set, chief!"
