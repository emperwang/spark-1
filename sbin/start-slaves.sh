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

# Starts a slave instance on each machine specified in the conf/slaves file.
# spark home的设置
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
# 运行环境的设置
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

# Find the port number for the master
# 设置默认的 master的端口号
if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi
# 默认的master的 host地址为 本机
if [ "$SPARK_MASTER_HOST" = "" ]; then
  case `uname` in
      (SunOS)
	  SPARK_MASTER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
	  ;;
      (*)
	  SPARK_MASTER_HOST="`hostname -f`"
	  ;;
  esac
fi
# 调用slaves.sh 来启动所有的slave; 其实就是ssh 到slave所在的机器,来执行sbin/start-slave.sh命令
# Launch the slaves
# 此处就是调用 slaves.sh脚本, 并把启动slave的命令作为参数传递进来
"${SPARK_HOME}/sbin/slaves.sh" cd "${SPARK_HOME}" \; "${SPARK_HOME}/sbin/start-slave.sh" "spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
