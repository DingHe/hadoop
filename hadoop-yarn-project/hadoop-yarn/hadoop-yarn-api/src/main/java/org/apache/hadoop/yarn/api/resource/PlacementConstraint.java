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

package org.apache.hadoop.yarn.api.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeAttributeOpCode;

/**
 * {@code PlacementConstraint} represents a placement constraint for a resource
 * allocation.
 */
//代表YARN资源分配时的调度约束，用于定义任务如何在集群节点上进行放置。通过该类，用户可以指定应用的分配规则，例如：
//亲和性 (Affinity)：任务必须与某些特定的资源或标签一起放置。
//反亲和性 (Anti-affinity)：任务不能与某些特定资源或标签一起放置。
//基于属性的约束：任务必须分配到具有特定属性的节点上。
//基于拓扑的约束：任务可以基于集群拓扑规则，例如在同一个节点、机架或其他层次结构中进行分配。
@Public
@Unstable
public class PlacementConstraint {

  /**
   * The constraint expression tree.
   */
  private AbstractConstraint constraintExpr;

  public PlacementConstraint(AbstractConstraint constraintExpr) {
    this.constraintExpr = constraintExpr;
  }

  @Override
  public String toString() {
    return this.constraintExpr.toString();
  }

  /**
   * Get the constraint expression of the placement constraint.
   *
   * @return the constraint expression
   */
  public AbstractConstraint getConstraintExpr() {
    return constraintExpr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PlacementConstraint)) {
      return false;
    }

    PlacementConstraint that = (PlacementConstraint) o;

    return getConstraintExpr() != null ? getConstraintExpr().equals(that
        .getConstraintExpr()) : that.getConstraintExpr() == null;
  }

  @Override
  public int hashCode() {
    return getConstraintExpr() != null ? getConstraintExpr().hashCode() : 0;
  }

  /**
   * Interface used to enable the elements of the constraint tree to be visited.
   */
  //访问者模式接口
  @Private
  public interface Visitable {
    /**
     * Visitor pattern.
     *
     * @param visitor visitor to be used
     * @param <T> defines the type that the visitor will use and the return type
     *          of the accept.
     * @return the result of visiting a given object.
     */
    <T> T accept(Visitor<T> visitor);

  }

  /**
   * Visitor API for a constraint tree.
   *
   * @param <T> determines the return type of the visit methods.
   */
  @Private
  public interface Visitor<T> {
    T visit(SingleConstraint constraint);

    T visit(TargetExpression target);

    T visit(TargetConstraint constraint);

    T visit(CardinalityConstraint constraint);

    T visit(And constraint);

    T visit(Or constraint);

    T visit(DelayedOr constraint);

    T visit(TimedPlacementConstraint constraint);
  }

  /**
   * Abstract class that acts as the superclass of all placement constraint
   * classes.
   */
  //所有调度约束的父类
  public abstract static class AbstractConstraint implements Visitable {
    public PlacementConstraint build() {
      return new PlacementConstraint(this);
    }

    @Override
    public String toString() {
      return super.toString();
    }
  }

  static final String NODE_SCOPE = "node";
  static final String RACK_SCOPE = "rack";

  /**
   * Consider a set of nodes N that belongs to the scope specified in the
   * constraint. If the target expressions are satisfied at least minCardinality
   * times and at most max-cardinality times in the node set N, then the
   * constraint is satisfied.
   *
   * For example, a constraint of the form {@code {RACK, 2, 10,
   * allocationTag("zk")}}, requires an allocation to be placed within a rack
   * that has at least 2 and at most 10 other allocations with tag "zk".
   */
  //表示在指定范围（scope）内，目标表达式（target expressions）出现的次数必须满足最小（min）和最大（max）限制
  public static class SingleConstraint extends AbstractConstraint {
    private String scope; //作用域（如 RACK、NODE），定义约束的适用范围
    private int minCardinality; //最小出现次数，必须至少 minCardinality 次
    private int maxCardinality; //最大出现次数，最多 maxCardinality 次
    private Set<TargetExpression> targetExpressions; //目标表达式，定义具体匹配的条件
    private NodeAttributeOpCode attributeOpCode; //节点属性运算符（可选），用于基于节点属性的调度

    public SingleConstraint(String scope, int minCardinality,
        int maxCardinality, NodeAttributeOpCode opCode,
        Set<TargetExpression> targetExpressions) {
      this.scope = scope;
      this.minCardinality = minCardinality;
      this.maxCardinality = maxCardinality;
      this.targetExpressions = targetExpressions;
      this.attributeOpCode = opCode;
    }

    public SingleConstraint(String scope, int minCardinality,
        int maxCardinality, Set<TargetExpression> targetExpressions) {
      this(scope, minCardinality, maxCardinality, NodeAttributeOpCode.NO_OP,
          targetExpressions);
    }

    public SingleConstraint(String scope, int minC, int maxC,
        TargetExpression... targetExpressions) {
      this(scope, minC, maxC, new HashSet<>(Arrays.asList(targetExpressions)));
    }

    public SingleConstraint(String scope, int minC, int maxC,
        NodeAttributeOpCode opCode,
        TargetExpression... targetExpressions) {
      this(scope, minC, maxC, opCode,
          new HashSet<>(Arrays.asList(targetExpressions)));
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the minimum cardinality of the constraint.
     *
     * @return the minimum cardinality of the constraint
     */
    public int getMinCardinality() {
      return minCardinality;
    }

    /**
     * Get the maximum cardinality of the constraint.
     *
     * @return the maximum cardinality of the constraint
     */
    public int getMaxCardinality() {
      return maxCardinality;
    }

    /**
     * Get the target expressions of the constraint.
     *
     * @return the set of target expressions
     */
    public Set<TargetExpression> getTargetExpressions() {
      return targetExpressions;
    }

    /**
     * Get the NodeAttributeOpCode of the constraint.
     *
     * @return nodeAttribute Op Code
     */
    public NodeAttributeOpCode getNodeAttributeOpCode() {
      return attributeOpCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SingleConstraint)) {
        return false;
      }

      SingleConstraint that = (SingleConstraint) o;

      if (getMinCardinality() != that.getMinCardinality()) {
        return false;
      }
      if (getMaxCardinality() != that.getMaxCardinality()) {
        return false;
      }
      if (!getScope().equals(that.getScope())) {
        return false;
      }
      if (getNodeAttributeOpCode() != null && !getNodeAttributeOpCode()
          .equals(that.getNodeAttributeOpCode())) {
        return false;
      }
      return getTargetExpressions().equals(that.getTargetExpressions());
    }

    @Override
    public int hashCode() {
      int result = getScope().hashCode();
      result = 31 * result + getMinCardinality();
      result = 31 * result + getMaxCardinality();
      result = 31 * result + getNodeAttributeOpCode().hashCode();
      result = 31 * result + getTargetExpressions().hashCode();
      return result;
    }

    @Override
    public String toString() {
      int max = getMaxCardinality();
      int min = getMinCardinality();
      List<String> targetExprList = getTargetExpressions().stream()
          .map(TargetExpression::toString).collect(Collectors.toList());
      List<String> targetConstraints = new ArrayList<>();
      for (String targetExpr : targetExprList) {
        if (min == 0 && max == 0) {
          // anti-affinity
          targetConstraints.add(new StringBuilder()
              .append("notin").append(",")
              .append(getScope()).append(",")
              .append(targetExpr)
              .toString());
        } else if (min == 1 && max == Integer.MAX_VALUE) {
          // affinity
          targetConstraints.add(new StringBuilder()
              .append("in").append(",")
              .append(getScope()).append(",")
              .append(targetExpr)
              .toString());
        } else if (min == -1 && max == -1) {
          // node attribute
          targetConstraints.add(new StringBuilder()
              .append(getScope()).append(",")
              .append(getNodeAttributeOpCode()).append(",")
              .append(targetExpr)
              .toString());
        } else {
          // cardinality
          targetConstraints.add(new StringBuilder()
              .append("cardinality").append(",")
              .append(getScope()).append(",")
              .append(targetExpr).append(",")
              .append(min).append(",")
              .append(max)
              .toString());
        }
      }
      return String.join(":", targetConstraints);
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class representing the target expressions that are used in placement
   * constraints. They might refer to expressions on node attributes, allocation
   * tags, or be self-targets (referring to the allocation to which the
   * constraint is attached).
   */
  //用于描述节点属性或分配标签，这些表达式用于定义资源调度的放置约束。
  //在 YARN 的放置约束（PlacementConstraint）中，TargetExpression 可以被用于：
  //约束任务必须分配到具有某些节点属性的节点上（如 rack=us-west）。
  //约束任务必须分配到具有相同分配标签的节点上（如 zk 和 nn 任务必须在同一节点上）
  public static class TargetExpression implements Visitable {
    /**
     * Enum specifying the type of the target expression.
     */
    public enum TargetType {
      NODE_ATTRIBUTE, ALLOCATION_TAG
    }

    private TargetType targetType;//目标表达式的类型，可能是节点属性（NODE_ATTRIBUTE）或分配标签（ALLOCATION_TAG）
    private String targetKey; //在 NODE_ATTRIBUTE 模式下，代表节点属性的名称（如 rack、region）
    private Set<String> targetValues;//目标键可能的取值，例如 {us-west, us-east}

    public TargetExpression(TargetType targetType, String targetKey,
        Set<String> targetValues) {
      this.targetType = targetType;
      this.targetKey = targetKey;
      this.targetValues = targetValues;
    }

    public TargetExpression(TargetType targetType) {
      this(targetType, null, new HashSet<>());
    }

    public TargetExpression(TargetType targetType, String targetKey,
        String... targetValues) {
      this(targetType, targetKey, new HashSet<>(Arrays.asList(targetValues)));
    }

    /**
     * Get the type of the target expression.
     *
     * @return the type of the target expression
     */
    public TargetType getTargetType() {
      return targetType;
    }

    /**
     * Get the key of the target expression.
     *
     * @return the key of the target expression
     */
    public String getTargetKey() {
      return targetKey;
    }

    /**
     * Get the set of values of the target expression.
     *
     * @return the set of values of the target expression
     */
    public Set<String> getTargetValues() {
      return targetValues;
    }

    @Override
    public int hashCode() {
      int result = targetType != null ? targetType.hashCode() : 0;
      result = 31 * result + (targetKey != null ? targetKey.hashCode() : 0);
      result =
          31 * result + (targetValues != null ? targetValues.hashCode() : 0);
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null) {
        return false;
      }
      if (!(o instanceof TargetExpression)) {
        return false;
      }

      TargetExpression that = (TargetExpression) o;
      if (targetType != that.targetType) {
        return false;
      }
      if (targetKey != null ? !targetKey.equals(that.targetKey)
          : that.targetKey != null) {
        return false;
      }
      return targetValues != null ? targetValues.equals(that.targetValues)
          : that.targetValues == null;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      if (TargetType.ALLOCATION_TAG == this.targetType) {
        // following by a comma separated tags
        sb.append(String.join(",", getTargetValues()));
      } else if (TargetType.NODE_ATTRIBUTE == this.targetType) {
        // following by a comma separated key value pairs
        if (this.getTargetValues() != null) {
          String attributeName = this.getTargetKey();
          String attributeValues = String.join(":", this.getTargetValues());
          sb.append(attributeName + "=[" + attributeValues + "]");
        }
      }
      return sb.toString();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a target constraint. Such a constraint requires an
   * allocation to be placed within a scope that satisfies some specified
   * expressions on node attributes and allocation tags.
   *
   * It is a specialized version of the {@link SingleConstraint}, where the
   * minimum and the maximum cardinalities take specific values based on the
   * {@link TargetOperator} used.
   */
  //用于约束任务的放置是否符合某些目标（如位于特定范围内或不在特定范围内）
  public static class TargetConstraint extends AbstractConstraint {
    /**
     * TargetOperator enum helps to specify type.
     */
    enum TargetOperator {
      IN("in"), //IN 表示必须包含在目标范围内
      NOT_IN("notin"); //NOT_IN 表示不能包含在目标范围内

      private String operator;
      TargetOperator(String op) {
        this.operator = op;
      }

      String getOperator() {
        return this.operator;
      }
    }
    //目标操作符（IN / NOT_IN）
    private TargetOperator op;
    //约束作用范围（如 node 或 rack）
    private String scope;
    //目标表达式的集合
    private Set<TargetExpression> targetExpressions;

    public TargetConstraint(TargetOperator op, String scope,
        Set<TargetExpression> targetExpressions) {
      this.op = op;
      this.scope = scope;
      this.targetExpressions = targetExpressions;
    }

    /**
     * Get the target operator of the constraint.
     *
     * @return the target operator
     */
    public TargetOperator getOp() {
      return op;
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the set of target expressions.
     *
     * @return the set of target expressions
     */
    public Set<TargetExpression> getTargetExpressions() {
      return targetExpressions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TargetConstraint)) {
        return false;
      }

      TargetConstraint that = (TargetConstraint) o;

      if (getOp() != that.getOp()) {
        return false;
      }
      if (!getScope().equals(that.getScope())) {
        return false;
      }
      return getTargetExpressions().equals(that.getTargetExpressions());
    }

    @Override
    public int hashCode() {
      int result = getOp().hashCode();
      result = 31 * result + getScope().hashCode();
      result = 31 * result + getTargetExpressions().hashCode();
      return result;
    }

    @Override
    public String toString() {
      List<String> targetExprs = getTargetExpressions().stream().map(
          targetExpression -> new StringBuilder()
              .append(op.getOperator()).append(",")
              .append(scope).append(",")
              .append(targetExpression.toString())
              .toString()).collect(Collectors.toList());
      return String.join(":", targetExprs);
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }
  }

  /**
   * Class that represents a cardinality constraint. Such a constraint allows
   * the number of allocations with a specific set of tags and within a given
   * scope to be between some minimum and maximum values.
   *
   * It is a specialized version of the {@link SingleConstraint}, where the
   * target is a set of allocation tags.
   */
  // 基数约束（Cardinality Constraint），
  // 用于限制具有特定标签的任务实例数 在某个作用域 (scope) 内的最小值 (minCardinality) 和最大值 (maxCardinality)
  // 规定某个分配标签 (allocationTags) 的任务数应该在 [minCardinality, maxCardinality] 之间
  public static class CardinalityConstraint extends AbstractConstraint {
    //约束的作用域（如节点、机架、数据中心等）
    private String scope;
    //约束的最小任务数（下界）
    private int minCardinality;
    //约束的最大任务数（上界）。
    private int maxCardinality;
    //约束适用的标签集合。
    private Set<String> allocationTags;

    public CardinalityConstraint(String scope, int minCardinality,
        int maxCardinality, Set<String> allocationTags) {
      this.scope = scope;
      this.minCardinality = minCardinality;
      this.maxCardinality = maxCardinality;
      this.allocationTags = allocationTags;
    }

    /**
     * Get the scope of the constraint.
     *
     * @return the scope of the constraint
     */
    public String getScope() {
      return scope;
    }

    /**
     * Get the minimum cardinality of the constraint.
     *
     * @return the minimum cardinality of the constraint
     */
    public int getMinCardinality() {
      return minCardinality;
    }

    /**
     * Get the maximum cardinality of the constraint.
     *
     * @return the maximum cardinality of the constraint
     */
    public int getMaxCardinality() {
      return maxCardinality;
    }

    /**
     * Get the allocation tags of the constraint.
     *
     * @return the allocation tags of the constraint
     */
    public Set<String> getAllocationTags() {
      return allocationTags;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CardinalityConstraint that = (CardinalityConstraint) o;

      if (minCardinality != that.minCardinality) {
        return false;
      }
      if (maxCardinality != that.maxCardinality) {
        return false;
      }
      if (scope != null ? !scope.equals(that.scope) : that.scope != null) {
        return false;
      }
      return allocationTags != null ? allocationTags.equals(that.allocationTags)
          : that.allocationTags == null;
    }

    @Override
    public int hashCode() {
      int result = scope != null ? scope.hashCode() : 0;
      result = 31 * result + minCardinality;
      result = 31 * result + maxCardinality;
      result = 31 * result
          + (allocationTags != null ? allocationTags.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("cardinality").append(",").append(getScope()).append(",");
      for (String tag : getAllocationTags()) {
        sb.append(tag).append(",");
      }
      sb.append(minCardinality).append(",").append(maxCardinality);
      return sb.toString();
    }
  }

  /**
   * Class that represents composite constraints, which comprise other
   * constraints, forming a constraint tree.
   *
   * @param <R> the type of constraints that are used as children of the
   *          specific composite constraint
   */
  //组合约束，由其他约束组成
  public abstract static class CompositeConstraint<R extends Visitable>
      extends AbstractConstraint {

    /**
     * Get the children of this composite constraint.
     *
     * @return the children of the composite constraint
     */
    public abstract List<R> getChildren();

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return getChildren() != null ? getChildren().equals(
          ((CompositeConstraint)o).getChildren()) :
          ((CompositeConstraint)o).getChildren() == null;
    }

    @Override
    public int hashCode() {
      return getChildren() != null ? getChildren().hashCode() : 0;
    }
  }

  /**
   * Class that represents a composite constraint that is a conjunction of other
   * constraints.
   */
  //And 类表示 多个调度约束的合取（AND 关系），即一个应用的调度必须同时满足多个子约束
  public static class And extends CompositeConstraint<AbstractConstraint> {
    //存储所有子约束，每个子约束都是 AbstractConstraint 的实例
    private List<AbstractConstraint> children;

    public And(List<AbstractConstraint> children) {
      this.children = children;
    }

    public And(AbstractConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<AbstractConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("and(");
      Iterator<AbstractConstraint> it = getChildren().iterator();
      while (it.hasNext()) {
        AbstractConstraint child = it.next();
        sb.append(child.toString());
        if (it.hasNext()) {
          sb.append(":");
        }
      }
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * Class that represents a composite constraint that is a disjunction of other
   * constraints.
   */
  public static class Or extends CompositeConstraint<AbstractConstraint> {
    private List<AbstractConstraint> children;

    public Or(List<AbstractConstraint> children) {
      this.children = children;
    }

    public Or(AbstractConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<AbstractConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("or(");
      Iterator<AbstractConstraint> it = getChildren().iterator();
      while (it.hasNext()) {
        AbstractConstraint child = it.next();
        sb.append(child.toString());
        if (it.hasNext()) {
          sb.append(":");
        }
      }
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * Class that represents a composite constraint that comprises a list of timed
   * placement constraints (see {@link TimedPlacementConstraint}). The scheduler
   * should try to satisfy first the first timed child constraint within the
   * specified time window. If this is not possible, it should attempt to
   * satisfy the second, and so on.
   */
    //用于调度系统中带时间窗口的任务放置约束
    //调度器按顺序尝试满足多个定时放置约束
    //尝试满足 children 列表中的第一个 TimedPlacementConstraint
    //如果 无法满足，则尝试 下一个 约束，以此类推
  public static class DelayedOr
      extends CompositeConstraint<TimedPlacementConstraint> {
    private List<TimedPlacementConstraint> children = new ArrayList<>();

    public DelayedOr(List<TimedPlacementConstraint> children) {
      this.children = children;
    }

    public DelayedOr(TimedPlacementConstraint... children) {
      this(Arrays.asList(children));
    }

    @Override
    public List<TimedPlacementConstraint> getChildren() {
      return children;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("DelayedOr(");
      Iterator<TimedPlacementConstraint> it = getChildren().iterator();
      while (it.hasNext()) {
        TimedPlacementConstraint child = it.next();
        sb.append(child.toString());
        if (it.hasNext()) {
          sb.append(",");
        }
      }
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * Represents a timed placement constraint that has to be satisfied within a
   * time window.
   */
  //代表带时间限制的放置约束，用于在一定时间窗口内，确保某些调度约束得到满足。这在 Apache YARN 资源调度中可能用于：
  //任务必须在一定时间内分配到特定类型的节点上。
  //任务必须在一定调度尝试次数内满足约束
  public static class TimedPlacementConstraint implements Visitable {
    /**
     * The unit of scheduling delay.
     */
    public enum DelayUnit {
      MILLISECONDS,//（毫秒）：基于时间的调度约束
      OPPORTUNITIES//（调度机会）：基于调度尝试次数的约束
    }

    private AbstractConstraint constraint;//表示需要满足的放置约束（如节点属性、任务标签）
    private long schedulingDelay; //表示时间窗口的长度
    private DelayUnit delayUnit;//表示调度延迟的单位

    public TimedPlacementConstraint(AbstractConstraint constraint,
        long schedulingDelay, DelayUnit delayUnit) {
      this.constraint = constraint;
      this.schedulingDelay = schedulingDelay;
      this.delayUnit = delayUnit;
    }

    public TimedPlacementConstraint(AbstractConstraint constraint,
        long schedulingDelay) {
      this(constraint, schedulingDelay, DelayUnit.MILLISECONDS);
    }

    public TimedPlacementConstraint(AbstractConstraint constraint) {
      this(constraint, Long.MAX_VALUE, DelayUnit.MILLISECONDS);
    }

    /**
     * Get the constraint that has to be satisfied within the time window.
     *
     * @return the constraint to be satisfied
     */
    public AbstractConstraint getConstraint() {
      return constraint;
    }

    /**
     * Sets the constraint that has to be satisfied within the time window.
     *
     * @param constraint the constraint to be satisfied
     */
    public void setConstraint(AbstractConstraint constraint) {
      this.constraint = constraint;
    }

    /**
     * Get the scheduling delay value that determines the time window within
     * which the constraint has to be satisfied.
     *
     * @return the value of the scheduling delay
     */
    public long getSchedulingDelay() {
      return schedulingDelay;
    }

    /**
     * The unit of the scheduling delay.
     *
     * @return the unit of the delay
     */
    public DelayUnit getDelayUnit() {
      return delayUnit;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TimedPlacementConstraint that = (TimedPlacementConstraint) o;

      if (schedulingDelay != that.schedulingDelay) {
        return false;
      }
      if (constraint != null ? !constraint.equals(that.constraint) :
          that.constraint != null) {
        return false;
      }
      return delayUnit == that.delayUnit;
    }

    @Override
    public int hashCode() {
      int result = constraint != null ? constraint.hashCode() : 0;
      result = 31 * result + (int) (schedulingDelay ^ (schedulingDelay >>> 32));
      result = 31 * result + (delayUnit != null ? delayUnit.hashCode() : 0);
      return result;
    }
  }
}
