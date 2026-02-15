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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * {@code ReservationAllocationState} represents the reservation that is
 * made by a user.
 * <p>
 * It includes:
 * <ul>
 *   <li>Duration of the reservation.</li>
 *   <li>Acceptance time of the duration.</li>
 *   <li>
 *       List of {@link ResourceAllocationRequest}, which includes the time
 *       interval, and capability of the allocation.
 *       {@code ResourceAllocationRequest} represents an allocation
 *       made for a reservation for the current state of the queue. This can be
 *       changed for reasons such as re-planning, but will always be subject to
 *       the constraints of the user contract as described by
 *       {@link ReservationDefinition}
 *   </li>
 *   <li>{@link ReservationId} of the reservation.</li>
 *   <li>{@link ReservationDefinition} used to make the reservation.</li>
 * </ul>
 *
 * @see ResourceAllocationRequest
 * @see ReservationId
 * @see ReservationDefinition
 */
//代表 YARN 资源管理 中 用户创建的资源预留（Reservation）。它用于 跟踪和管理 资源预留的状态
  //在 YARN 容量调度器（Capacity Scheduler） 和 公平调度器（Fair Scheduler） 的 资源预留（Reservation） 机制中，
// ReservationAllocationState 起到了 记录和管理资源分配情况 的关键作用
  //资源预留（Reservation）允许用户预先申请计算资源（CPU、内存等），确保在 未来某个时间段 能够获得这些资源来运行任务。它通常用于 大规模作业
//资源预留的目标：
  //保证任务的执行：防止因资源不足导致任务被无限推迟
  //优化资源利用率：通过提前规划，提高集群整体吞吐量
  //支持长时间运行任务：确保复杂任务可以跨多个时间片顺利执行

@Public
@Stable
public abstract class ReservationAllocationState {

  /**
   *
   * @param acceptanceTime The acceptance time of the reservation.
   * @param user The username of the user who made the reservation.
   * @param resourceAllocations List of {@link ResourceAllocationRequest}
   *                            representing the current state of the
   *                            reservation resource allocations. This is
   *                            subject to change in the event of re-planning.
   * @param reservationId {@link ReservationId } of the reservation being
   *                                            listed.
   * @param reservationDefinition {@link ReservationDefinition} used to make
   *                              the reservation.
   * @return {@code ReservationAllocationState} that represents the state of
   * the reservation.
   */
  @Public
  @Stable
  public static ReservationAllocationState newInstance(long acceptanceTime,
           String user, List<ResourceAllocationRequest> resourceAllocations,
           ReservationId reservationId,
           ReservationDefinition reservationDefinition) {
    ReservationAllocationState ri = Records.newRecord(
            ReservationAllocationState.class);
    ri.setAcceptanceTime(acceptanceTime);//记录 资源预留被接受的时间，表示该预留什么时候被批准
    ri.setUser(user);//记录 预留资源的用户，用于身份识别和访问控制
    ri.setResourceAllocationRequests(resourceAllocations);//记录 预留的资源分配情况，其中 ResourceAllocationRequest 详细描述了 每个时间段 的资源分配
    ri.setReservationId(reservationId);//记录 该次资源预留的唯一 ID，用于唯一标识该预留
    ri.setReservationDefinition(reservationDefinition); //记录 该次资源预留的定义，例如资源需求、时间范围等
    return ri;
  }

  /**
   * Get the acceptance time of the reservation.
   *
   * @return the time that the reservation was accepted.
   */
  @Public
  @Unstable
  public abstract long getAcceptanceTime();

  /**
   * Set the time that the reservation was accepted.
   *
   * @param acceptanceTime The acceptance time of the reservation.
   */
  @Private
  @Unstable
  public abstract void setAcceptanceTime(long acceptanceTime);

  /**
   * Get the user who made the reservation.
   *
   * @return the name of the user who made the reservation.
   */
  @Public
  @Unstable
  public abstract String getUser();

  /**
   * Set the user who made the reservation.
   *
   * @param user The username of the user who made the reservation.
   */
  @Private
  @Unstable
  public abstract void setUser(String user);

  /**
   * Get the Resource allocations of the reservation based on the current state
   * of the plan. This is subject to change in the event of re-planning.
   * The allocations will be constraint to the user contract as described by
   * the {@link ReservationDefinition}
   *
   * @return a list of resource allocations for the reservation.
   */
  @Public
  @Unstable
  public abstract List<ResourceAllocationRequest>
          getResourceAllocationRequests();

  /**
   * Set the list of resource allocations made for the reservation.
   *
   * @param resourceAllocations List of {@link ResourceAllocationRequest}
   *                            representing the current state of the
   *                            reservation resource allocations. This is
   *                            subject to change in the event of re-planning.
   */
  @Private
  @Unstable
  public abstract void setResourceAllocationRequests(
          List<ResourceAllocationRequest> resourceAllocations);

  /**
   * Get the id of the reservation.
   *
   * @return the reservation id corresponding to the reservation.
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * Set the id corresponding to the reservation.
   * `
   * @param reservationId {@link ReservationId } of the reservation being
   *                                            listed.
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId reservationId);

  /**
   * Get the reservation definition used to make the reservation.
   *
   * @return the reservation definition used to make the reservation.
   */
  @Public
  @Unstable
  public abstract ReservationDefinition getReservationDefinition();

  /**
   * Set the definition of the reservation.
   *
   * @param reservationDefinition {@link ReservationDefinition} used to make
   *                              the reservation.
   */
  @Private
  @Unstable
  public abstract void setReservationDefinition(ReservationDefinition
                                                      reservationDefinition);


}
