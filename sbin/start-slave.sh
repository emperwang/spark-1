#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts a slave on the machine this script is executed on.
#
# Environment Variables
#
#   SPARK_WORKER_INSTANCES  The number of worker instances to run on this
#                           slave.  Default is 1.
#   SPARK_WORKER_PORT       The base port number for the first worker. If set,
#                           subsequent workers will increment this number.  If
#                           unset, Spark will find a valid port number, but
#                           with no guarantee of a predictable pattern.
#   SPARK_WORKER_WEBUI_PORT The base port for the web interface of the first
#                           worker.  Subsequent workers will increment this
#                           number.  Default is 8081.
# spark home的配置
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.

# worker 的class
CLASS="org.apache.spark.deploy.worker.Worker"
#  如果没有给参数 或者 参数给出的是 --help 或 -h  则打印 帮助信息
if [[ $# -lt 1 ]] || [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-slave.sh <master> [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Registered signal handlers for"
  # 可见帮助信息是由  spark-class $CLASS产生的
  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi
# 运行环境的配置
. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

# First argument should be the master; we need to store it aside because we may
# need to insert arguments between it and the other arguments
# 记录 命令行传递的 master的地址
MASTER=$1
shift

# Determine desired worker port
# 配置默认的 slave的webui的端口号
if [ "$SPARK_WORKER_WEBUI_PORT" = "" ]; then
  SPARK_WORKER_WEBUI_PORT=8081
fi

# Start up the appropriate number of workers on this machine.
# quick local function to start a worker
# 启动实例的函数
function start_instance {
  # 要启动的实例的 num
  WORKER_NUM=$1
  shift
  #
  if [ "$SPARK_WORKER_PORT" = "" ]; then
    PORT_FLAG=
    PORT_NUM=
  else
    PORT_FLAG="--port"
    PORT_NUM=$(( $SPARK_WORKER_PORT + $WORKER_NUM - 1 ))
  fi
  # webui的端口; 当启动多个worker 实例时, 端口号往后移动
  WEBUI_PORT=$(( $SPARK_WORKER_WEBUI_PORT + $WORKER_NUM - 1 ))
  # 启动
  "${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS $WORKER_NUM \
     --webui-port "$WEBUI_PORT" $PORT_FLAG $PORT_NUM $MASTER "$@"
}
# 如果没有配置 SPARK_WORKER_INSTANCES 则默认的worker的实例数 为1
if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  start_instance 1 "$@"
else
  # 如果配置了 SPARK_WORKER_INSTANCES 则循环启动多个 worker实例
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    start_instance $(( 1 + $i )) "$@"
  done
fi
