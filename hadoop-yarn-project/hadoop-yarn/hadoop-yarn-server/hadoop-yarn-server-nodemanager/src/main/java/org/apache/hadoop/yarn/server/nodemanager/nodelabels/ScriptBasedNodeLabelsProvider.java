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

package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.io.IOException;
import java.util.Set;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * The class which provides functionality of getting the labels of the node
 * using the configured node labels provider script. "NODE_PARTITION:" is the
 * pattern which will be used to search node label partition from the out put of
 * the NodeLabels provider script
 */
//供了通过配置的脚本来获取节点标签的功能。它通过运行外部脚本来获取节点标签，并从脚本的输出中解析出标签信息
public class ScriptBasedNodeLabelsProvider extends NodeLabelsProvider {

  /** Pattern used for searching in the output of the node labels script */
  //定义了在脚本输出中查找节点标签的模式
  public static final String NODE_LABEL_PARTITION_PATTERN = "NODE_PARTITION:";

  private NodeDescriptorsScriptRunner runner;

  public ScriptBasedNodeLabelsProvider() {
    super(ScriptBasedNodeLabelsProvider.class.getName());
  }

  /*
   * Method which initializes the values for the script path and interval time.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    //从 Configuration 对象中获取脚本路径、脚本超时时间、脚本参数和任务间隔时间
    String nodeLabelsScriptPath =
        conf.get(YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_PATH);
    long scriptTimeout =
        conf.getLong(YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_TIMEOUT_MS);
    String[] scriptArgs = conf.getStrings(
        YarnConfiguration.NM_SCRIPT_BASED_NODE_LABELS_PROVIDER_SCRIPT_OPTS,
        new String[] {});
    //验证脚本路径是否有效
    verifyConfiguredScript(nodeLabelsScriptPath);

    long taskInterval = conf.getLong(
        YarnConfiguration.NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_NODE_LABELS_PROVIDER_FETCH_INTERVAL_MS);
    this.setIntervalTime(taskInterval);
    //初始化 NodeLabelScriptRunner，该执行器用于执行脚本并解析输出
    this.runner = new NodeLabelScriptRunner(nodeLabelsScriptPath, scriptArgs,
            scriptTimeout, this);

    super.serviceInit(conf);
  }

  /**
   * Method used to terminate the Node Labels Fetch script.
   */
  @Override
  public void cleanUp() {
    if (runner != null) {
      runner.cleanUp();
    }
  }

  // A script runner periodically runs a script to get node labels,
  // and sets these labels to the given provider.
  private static class NodeLabelScriptRunner extends
      NodeDescriptorsScriptRunner<NodeLabel> {

    NodeLabelScriptRunner(String scriptPath, String[] scriptArgs,
        long scriptTimeout, ScriptBasedNodeLabelsProvider provider) {
      super(scriptPath, scriptArgs, scriptTimeout, provider);
    }

    /**
     * Method which collect lines from the output string which begins with
     * Patterns provided.
     *
     * @param scriptOutput string
     * @return true if output string has error pattern in it.
     * @throws IOException
     */
    @Override
    Set<NodeLabel> parseOutput(String scriptOutput)
        throws IOException {
      String nodePartitionLabel = null;
      String[] splits = scriptOutput.split("\n");
      for (String line : splits) {
        String trimmedLine = line.trim();
        if (trimmedLine.startsWith(NODE_LABEL_PARTITION_PATTERN)) {
          nodePartitionLabel =
              trimmedLine.substring(NODE_LABEL_PARTITION_PATTERN.length());
        }
      }
      return convertToNodeLabelSet(nodePartitionLabel);
    }
  }

  @Override
  public TimerTask createTimerTask() {
    return runner;
  }
}
