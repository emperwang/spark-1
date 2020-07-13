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

# Runs a Spark command as a daemon.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
#   SPARK_MASTER    host:path where spark code should be rsync'd from
#   SPARK_PID_DIR   The pid files are stored. /tmp by default.
#   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
#   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
#   SPARK_NO_DAEMONIZE   If set, will run the proposed command in the foreground. It will not output a PID file.
##
# 使用方法
usage="Usage: spark-daemon.sh [--config <conf-dir>] (start|stop|submit|status) <spark-command> <spark-instance-number> <args...>"

# if no args specified, show usage
# 如果没有传递参数,则打印使用方法
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi
# 设置SPARK_HOME,没有设置的话,则设置为当前目录的上级目录的全路径
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
# 配置脚本
. "${SPARK_HOME}/sbin/spark-config.sh"

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
# 由此可见,可以通过--config 参数来配置配置文件的目录
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi

# 启动master 传递的参数
# spark-daemon.sh start $CLASS 1 \
#  --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
#  $ORIGINAL_ARGS
# 此三个变量,分别获取 start  #CLASS  1
option=$1
shift
command=$1
shift
instance=$1
shift
# 滚动日志
spark_rotate_log ()
{
  # 记录文件名
    log=$1;
    # 滚动的个数
    num=5;
    # 第二个参数 表示 通过参数2 可以指定 num的值
    if [ -n "$2" ]; then
	    num=$2
    fi
    # 存在日志文件,则进行滚动操作
    if [ -f "$log" ]; then # rotate logs
    # 滚动操作, 备份文件; 此赋值会覆盖; 最终日志文件就是 num个
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}
# 执行env,配置运行环境
. "${SPARK_HOME}/bin/load-spark-env.sh"
# 执行此脚本的 用户
if [ "$SPARK_IDENT_STRING" = "" ]; then
  export SPARK_IDENT_STRING="$USER"
fi


export SPARK_PRINT_LAUNCH_COMMAND="1"

# get log directory
# 配置日志目录
if [ "$SPARK_LOG_DIR" = "" ]; then
  export SPARK_LOG_DIR="${SPARK_HOME}/logs"
fi
# 创建日志目录
mkdir -p "$SPARK_LOG_DIR"
# 创建一个隐形文件进行测试
touch "$SPARK_LOG_DIR"/.spark_test > /dev/null 2>&1
# 获取测试的结果
TEST_LOG_DIR=$?
# 在日志目录创建文件成功, 则进行杀出
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$SPARK_LOG_DIR"/.spark_test
else  # 给目录切换属主
  chown "$SPARK_IDENT_STRING" "$SPARK_LOG_DIR"
fi
# 存储进程id的文件目录
if [ "$SPARK_PID_DIR" = "" ]; then
  SPARK_PID_DIR=/tmp
fi

# some variables
# 日志以及pid文件的名字指定
log="$SPARK_LOG_DIR/spark-$SPARK_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$SPARK_PID_DIR/spark-$SPARK_IDENT_STRING-$command-$instance.pid"

# Set default scheduling priority
# 调度优先级
if [ "$SPARK_NICENESS" = "" ]; then
    export SPARK_NICENESS=0
fi
# 执行命令
# execute_command nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class "$command" "$@"
# execute_command nice -n "$SPARK_NICENESS" bash "${SPARK_HOME}"/bin/spark-submit --class "$command" "$@"
execute_command() {
  #SPARK_NO_DAEMONIZE 表示前台启动,不会生成pid文件
  # ${SPARK_NO_DAEMONIZE+set}  表示SPARK_NO_DAEMONIZE此声明了,值就是set,没有声明,则为null
  # 此处是,没有设置,就运行then; 设置了运行else
  if [ -z ${SPARK_NO_DAEMONIZE+set} ]; then
    # 这个启动命令 很有意思
      nohup -- "$@" >> $log 2>&1 < /dev/null &
      newpid="$!"

      echo "$newpid" > "$pid"

      # Poll for up to 5 seconds for the java process to start
      # 等待 5s ,让java程序启动
      for i in {1..10}
      do
        if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
           break
        fi
        sleep 0.5
      done

      sleep 2
      # Check if the process has died; in that case we'll tail the log so the user can see
      # 如果启动失败,则打印日志文件的最后10行
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch: $@"
        tail -10 "$log" | sed 's/^/  /'
        echo "full log in $log"
      fi
  else
      # 启动启动程序
      "$@"
  fi
}

run_command() {
  # mode 记录模式,是运行class 还是提交任务
  mode="$1"
  shift
  # 创建pid目录
  mkdir -p "$SPARK_PID_DIR"
  # 检测pid 文件是否存在
  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then   # pid文件存在,且程序在运行,则输出提示信息
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi
  # 数据的同步, 从 spark-master机器 把 master的文件同步过来
  if [ "$SPARK_MASTER" != "" ]; then
    echo rsync from "$SPARK_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
  fi
  # 滚动日志
  spark_rotate_log "$log"
  echo "starting $command, logging to $log"
  # 根据模式  执行不同的操作
  case "$mode" in
    (class)
      execute_command nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class "$command" "$@"
      ;;

    (submit)
      execute_command nice -n "$SPARK_NICENESS" bash "${SPARK_HOME}"/bin/spark-submit --class "$command" "$@"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

}

case $option in
  # 提交任务时,执行的命令
  (submit)
    run_command submit "$@"
    ;;
  # 启动执行的命令
  (start)
    run_command class "$@"
    ;;
  # 停止执行的目录
  (stop)
  # 先检测 存储pid的文件是否存在
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"  # 文件存在,则获取进程的pid号
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then  # 如果是个java程序,则继续执行
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"   # 使用kill 停止程序, 并删除 pid文件
      else
        echo "no $command to stop"  # 如果不是java程序,则说明程序没有在运行
      fi
    else
      echo "no $command to stop"   # pid文件不存在,说明 程序没有在运行
    fi
    ;;
  # 检测状态
  (status)
  # 先判断 pid文件
    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then # 文件存在,程序在运行,则正常
        echo $command is running.
        exit 0
      else      # 文件存在,但是程序不再,则没有运行
        echo $pid file is present but $command not running
        exit 1
      fi
    else  # pid 文件不存在, 说明程序就没有运行
      echo $command not running.
      exit 2
    fi
    ;;
  # 输入其他参数,则打印 帮助信息
  (*)
    echo $usage
    exit 1
    ;;

esac


