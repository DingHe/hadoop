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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AppPriorityACLGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * Manager class to store and check permission for Priority ACLs.
 */
//管理和检查应用程序提交时的优先级访问控制列表（ACLs）。
// 它负责为每个队列存储和检查与优先级相关的权限，以确保用户在提交应用程序时具有足够的权限来指定应用程序的优先级。
// 如果启用了 ACL 功能，类将根据预配置的 ACL 检查用户是否有权限提交具有特定优先级的应用程序
public class AppPriorityACLsManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(AppPriorityACLsManager.class);

  /*
   * An internal class to store ACLs specific to each priority. This will be
   * used to read and process acl's during app submission time as well.
   */
  //用于表示和管理与优先级相关的访问控制列表（ACL）。它包含了与特定优先级相关的配置项，如优先级、本地优先级和 ACL。
  private static class PriorityACL {
    //表示该 ACL 配置的最大优先级。用于确定在某一优先级下，用户是否具有权限
    private Priority priority;
    //当用户没有指定优先级时，使用默认优先级。对于没有设置优先级的应用程序，会使用此优先级
    private Priority defaultPriority;
    //表示与优先级关联的访问控制列表（ACL）
    private AccessControlList acl;

    PriorityACL(Priority priority, Priority defaultPriority,
        AccessControlList acl) {
      this.setPriority(priority);
      this.setDefaultPriority(defaultPriority);
      this.setAcl(acl);
    }

    public Priority getPriority() {
      return priority;
    }

    public void setPriority(Priority maxPriority) {
      this.priority = maxPriority;
    }

    public Priority getDefaultPriority() {
      return defaultPriority;
    }

    public void setDefaultPriority(Priority defaultPriority) {
      this.defaultPriority = defaultPriority;
    }

    public AccessControlList getAcl() {
      return acl;
    }

    public void setAcl(AccessControlList acl) {
      this.acl = acl;
    }
  }
  //表示是否启用了 ACL 功能。如果为 false，则表示 ACL 功能被禁用，所有用户都可以提交应用程序并设置优先级，不进行权限检查。默认为 true，表示启用了 ACL 功能
  private boolean isACLsEnable;
  //存储每个队列与其对应的优先级 ACL 列表。键是队列名，值是该队列所有配置的 PriorityACL 列表
  private final ConcurrentMap<String, List<PriorityACL>> allAcls =
      new ConcurrentHashMap<>();

  public AppPriorityACLsManager(Configuration conf) {
    this.isACLsEnable = conf.getBoolean(YarnConfiguration.YARN_ACL_ENABLE,
        YarnConfiguration.DEFAULT_YARN_ACL_ENABLE);
  }

  /**
   * Clear priority acl during refresh.
   *
   * @param queueName
   *          Queue Name
   */
  public void clearPriorityACLs(String queueName) {
    allAcls.remove(queueName);
  }

  /**
   * Each Queue could have configured with different priority acl's groups. This
   * method helps to store each such ACL list against queue.
   *
   * @param priorityACLGroups
   *          List of Priority ACL Groups.
   * @param queueName
   *          Queue Name associate with priority acl groups.
   */
  //用于将不同优先级的访问控制列表（ACL）组添加到指定的队列中。每个队列可能会有多个优先级的 ACL 配置，而这个方法帮助存储每个优先级组与队列的关联
  public void addPrioirityACLs(List<AppPriorityACLGroup> priorityACLGroups,
      String queueName) {

    List<PriorityACL> priorityACL = allAcls.get(queueName);
    if (null == priorityACL) {
      priorityACL = new ArrayList<PriorityACL>();
      allAcls.put(queueName, priorityACL);
    }

    // Ensure lowest priority PriorityACLGroup comes first in the list.
    //排序优先级组
    Collections.sort(priorityACLGroups);
    //将优先级 ACL 组添加到队列的 ACL 列表中
    for (AppPriorityACLGroup priorityACLGroup : priorityACLGroups) {
      priorityACL.add(new PriorityACL(priorityACLGroup.getMaxPriority(),
          priorityACLGroup.getDefaultPriority(),
          priorityACLGroup.getACLList()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Priority ACL group added: max-priority - "
            + priorityACLGroup.getMaxPriority() + "default-priority - "
            + priorityACLGroup.getDefaultPriority());
      }
    }
  }

  /**
   * Priority based checkAccess to ensure that given user has enough permission
   * to submit application at a given priority level.
   *
   * @param callerUGI
   *          User who submits the application.
   * @param queueName
   *          Queue to which application is submitted.
   * @param submittedPriority
   *          priority of the application.
   * @return True or False to indicate whether application can be submitted at
   *         submitted priority level or not.
   */
  // 用于检查给定的用户是否具有足够的权限，以便在特定的优先级水平上提交应用程序。
  // 这个方法会根据用户、队列名和提交的优先级来验证访问权限，确保用户能够以该优先级提交任务
  //submittedPriority 该参数表示应用程序请求的优先级，系统将检查该优先级是否符合该用户在该队列中提交应用程序的权限
  public boolean checkAccess(UserGroupInformation callerUGI, String queueName,
      Priority submittedPriority) {
    if (!isACLsEnable) {
      return true;
    }

    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return true;
    }

    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls,
        callerUGI, submittedPriority);
    if (approvedPriorityACL == null) {
      return false;
    }

    return true;
  }

  /**
   * If an application is submitted without any priority, and submitted user has
   * a default priority, this method helps to update this default priority as
   * app's priority.
   *
   * @param queueName
   *          Submitted queue
   * @param user
   *          User who submitted this application
   * @return Default priority associated with given user.
   */
  public Priority getDefaultPriority(String queueName,
      UserGroupInformation user) {
    if (!isACLsEnable) {
      return null;
    }

    List<PriorityACL> acls = allAcls.get(queueName);
    if (acls == null || acls.isEmpty()) {
      return null;
    }

    PriorityACL approvedPriorityACL = getMappedPriorityAclForUGI(acls, user,
        null);
    if (approvedPriorityACL == null) {
      return null;
    }

    Priority defaultPriority = Priority
        .newInstance(approvedPriorityACL.getDefaultPriority().getPriority());
    return defaultPriority;
  }
  //用于从给定的 ACL 列表中为指定用户查找适用的优先级权限控制。它会根据优先级配置和用户权限来确定该用户是否可以在该优先级下提交应用程序
  private PriorityACL getMappedPriorityAclForUGI(List<PriorityACL> acls ,
      UserGroupInformation user, Priority submittedPriority) {

    // Iterate through all configured ACLs starting from lower priority.
    // If user is found corresponding to a configured priority, then store
    // that entry. if failed, continue iterate through whole acl list.
    //用于存储符合条件的优先级权限控制
    PriorityACL selectedAcl = null;
    for (PriorityACL entry : acls) {
      AccessControlList list = entry.getAcl();
      //检查用户是否被允许
      if (list.isUserAllowed(user)) {
        selectedAcl = entry;

        // If submittedPriority is passed through the argument, also check
        // whether submittedPriority is under max-priority of each ACL group.
        //如果提供了 submittedPriority，检查优先级是否合适
        if (submittedPriority != null) {
          selectedAcl = null;
          if (submittedPriority.getPriority() <= entry.getPriority()
              .getPriority()) {
            return entry;
          }
        }
      }
    }
    return selectedAcl;
  }
}
