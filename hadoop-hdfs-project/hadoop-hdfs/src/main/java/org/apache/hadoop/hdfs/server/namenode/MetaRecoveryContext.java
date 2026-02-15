/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Context data for an ongoing NameNode metadata recovery process. */
// 用于HDFS NameNode在进行元数据恢复过程中管理和控制用户的交互。
// 该类提供了一个上下文，用于在恢复过程中与用户进行交互，询问用户输入并根据用户选择进行相应的操作。
// 它的作用主要是在恢复过程中，根据用户的输入或设置，决定是否继续恢复、停止恢复或选择其他操作
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MetaRecoveryContext  {
  public static final Logger LOG =
      LoggerFactory.getLogger(MetaRecoveryContext.class.getName());
  public final static int FORCE_NONE = 0;//表示没有强制选择任何选项，恢复过程中需要用户进行输入
  public final static int FORCE_FIRST_CHOICE = 1;//表示强制选择第一个选项，用户输入的选项将被自动选择
  public final static int FORCE_ALL = 2;//表示强制选择所有选项，通常用于恢复过程中跳过所有提示，自动执行所有操作
  private int force;//决定了是否强制选择某个选项，而无需用户输入
  
  /** Exception thrown when the user has requested processing to stop. */
  static public class RequestStopException extends IOException {
    private static final long serialVersionUID = 1L;
    public RequestStopException(String msg) {
      super(msg);
    }
  }
  
  public MetaRecoveryContext(int force) {
    this.force = force;
  }

  /**
   * Display a prompt to the user and get his or her choice.
   *  
   * @param prompt      The prompt to display
   * @param firstChoice First choice (will be taken if autoChooseDefault is
   *                    true)
   * @param choices     Other choies
   *
   * @return            The choice that was taken
   * @throws IOException
   */
  //该方法用于向用户显示一个提示（prompt），并根据用户的输入获取一个选择。firstChoice 是第一个预设选项，如果 force 设置为非 FORCE_NONE，则会自动选择该选项
  public String ask(String prompt, String firstChoice, String... choices) 
      throws IOException {
    while (true) {
      System.err.print(prompt);
      if (force > FORCE_NONE) {
        System.out.println("automatically choosing " + firstChoice);
        return firstChoice;
      }
      StringBuilder responseBuilder = new StringBuilder();
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
      String response = responseBuilder.toString();
      if (response.equalsIgnoreCase(firstChoice))
        return firstChoice;
      for (String c : choices) {
        if (response.equalsIgnoreCase(c)) {
          return c;
        }
      }
      System.err.print("I'm sorry, I cannot understand your response.\n");
    }
  }
  //用于显示编辑日志加载的提示，并根据用户输入做出相应处理。提示包括继续（'c'）、停止（'s'）、退出（'q'）和总是选择第一个选项（'a'）
  public static void editLogLoaderPrompt(String prompt,
        MetaRecoveryContext recovery, String contStr)
        throws IOException, RequestStopException
  {
    if (recovery == null) {
      throw new IOException(prompt);
    }
    LOG.error(prompt);
    String answer = recovery.ask("\nEnter 'c' to continue, " + contStr + "\n" +
      "Enter 's' to stop reading the edit log here, abandoning any later " +
        "edits\n" +
      "Enter 'q' to quit without saving\n" +
      "Enter 'a' to always select the first choice in the future " +
      "without prompting. " + 
      "(c/s/q/a)\n", "c", "s", "q", "a");
    if (answer.equals("c")) {
      LOG.info("Continuing");
      return;
    } else if (answer.equals("s")) {
      throw new RequestStopException("user requested stop");
    } else if (answer.equals("q")) {
      recovery.quit();
    } else {
      recovery.setForce(FORCE_FIRST_CHOICE);
      return;
    }
  }

  /** Log a message and quit */
  public void quit() {
    LOG.error("Exiting on user request.");
    System.exit(0);
  }

  public int getForce() {
    return this.force;
  }

  public void setForce(int force) {
    this.force = force;
  }
}
