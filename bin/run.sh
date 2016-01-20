#!/bin/bash

if [ "$JAVA_HOME" = "" ]
then
   echo The JAVA_HOME environment variable needs to be set.
   exit 1
fi

if [ $# < 1 ]
then
  echo Usage: run.sh <path-to-env-properties> -n servername
  echo  For example:
  echo  numactl -m1 ./run.sh conf/multiproc-p2p/client.conf -n client
  exit 1
fi

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/.nvx/native

APPCONF=$1
shift

pushd `dirname $0`
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:`pwd`/native
rm -rf rdat
$JAVA_HOME/bin/java -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -Xms20g -Xmx20g  -XX:NewSize=1536m -XX:MaxNewSize=1536m -XX:SurvivorRatio=6 -XX:+UseParNewGC -XX:ParallelGCThreads=12 -Xnoclassgc -XX:MaxTenuringThreshold=2 -Dnv.app.propfile=$APPCONF -cp "libs/*" com.neeve.server.Main $*
popd
