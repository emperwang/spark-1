/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark scripts.
 * 命令行接口类,被spark script脚本使用
 */
class Main {

  /**
   *  使用方法说明:
   * Usage: Main [class] [class args]
   * <p>
   * This CLI works in two different modes:
   * 有两种模式:
   * <ul>
   *     spark-submit 使用,提交应用
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *    spark-class 使用, 启动程序
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * <p>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   */
  // 启动Master的的命令
  // java -Xmx128m -cp '/mnt/spark-alone/spark-2.4.6-bin-hadoop2.6/jars/*'
  // org.apache.spark.launcher.Main org.apache.spark.deploy.master.Master --host name2 --port 7077 --webui-port 8080
  public static void main(String[] argsArray) throws Exception {
    // 参数的个数检测
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");
    // 把命令行传递的参数 封装到一个 list中
    List<String> args = new ArrayList<>(Arrays.asList(argsArray));
    // 要操作的类
    String className = args.remove(0);
    //  是否打印 命令
    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    Map<String, String> env = new HashMap<>();
    List<String> cmd;
    // 如果操作的类是 SparkSubmit, 说明是提交任务
    // 这个先放到后面, 等分析到 SparkSubmit 再进行分析
    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        AbstractCommandBuilder builder = new SparkSubmitCommandBuilder(args);
        cmd = buildCommand(builder, env, printLaunchCommand);
      } catch (IllegalArgumentException e) {
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        try {
          parser.parse(args);
        } catch (Exception ignored) {
          // Ignore parsing exceptions.
        }

        List<String> help = new ArrayList<>();
        if (parser.className != null) {
          help.add(parser.CLASS);
          help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        AbstractCommandBuilder builder = new SparkSubmitCommandBuilder(help);
        cmd = buildCommand(builder, env, printLaunchCommand);
      }
    } else {  // spark-class的操作
      //
      AbstractCommandBuilder builder = new SparkClassCommandBuilder(className, args);
      // 创建命令
      cmd = buildCommand(builder, env, printLaunchCommand);
    }
    // 如果是windows系统,则进行此操作
    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // 不是windows系统,则进行此操作
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      // 针对unix系统,对构建好的命令,
      List<String> bashCmd = prepareBashCommand(cmd, env);
      // 把构建好的命令打印到终端
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0'); // \0 字符串的结束符
      }
    }
  }

  /**
   * Prepare spark commands with the appropriate command builder.
   * If printLaunchCommand is set then the commands will be printed to the stderr.
   */
  private static List<String> buildCommand(
      AbstractCommandBuilder builder,
      Map<String, String> env,
      boolean printLaunchCommand) throws IOException, IllegalArgumentException {
    // 调用builder 来构建命令
    List<String> cmd = builder.buildCommand(env);
    // 如果设置了打印命令,则把构建好的命令 打印到终端
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }
    return cmd;
  }

  /**
   * Prepare a command line for execution from a Windows batch script.
   *
   * The method quotes all arguments so that spaces are handled as expected. Quotes within arguments
   * are "double quoted" (which is batch for escaping a quote). This page has more details about
   * quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
   */
  private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
    StringBuilder cmdline = new StringBuilder();
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" && ");
    }
    for (String arg : cmd) {
      cmdline.append(quoteForBatchScript(arg));
      cmdline.append(" ");
    }
    return cmdline.toString();
  }

  /**
   * Prepare the command for execution from a bash script. The final command will have commands to
   * set up any needed environment variables needed by the child process.
   */
  private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
    if (childEnv.isEmpty()) {
      return cmd;
    }

    List<String> newCmd = new ArrayList<>();
    newCmd.add("env");
    // jvm参数的构建
    // 最终的命令
    //exec /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.232.b09-0.el7_7.x86_64/bin/java -cp
    // '/mnt/spark-alone/spark-2.4.6-bin-hadoop2.6/conf/:/mnt/spark-alone/spark-2.4.6-bin-hadoop2.6/jars/*'
    // -Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=name2:2181,name3:2181,name4:2181
    // -Dspark.deploy.zookeeper.dir=/spark-cluster
    //        -Xmx1g org.apache.spark.deploy.master.Master --host name2 --port 7077 --webui-port 8080
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  /**
   * A parser used when command line parsing fails for spark-submit. It's used as a best-effort
   * at trying to identify the class the user wanted to invoke, since that may require special
   * usage strings (handled by SparkSubmitArguments).
   */
  private static class MainClassOptionParser extends SparkSubmitOptionParser {

    String className;

    @Override
    protected boolean handle(String opt, String value) {
      if (CLASS.equals(opt)) {
        className = value;
      }
      return false;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
