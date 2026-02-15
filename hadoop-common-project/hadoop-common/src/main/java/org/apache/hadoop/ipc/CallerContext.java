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
package org.apache.hadoop.ipc;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_SEPARATOR_KEY;

/**
 * A class defining the caller context for auditing coarse granularity
 * operations.
 * 记录和传递调用者的上下文信息，主要应用于审计和跟踪 Hadoop 文件系统（HDFS）中的操作。该类是不可变类（immutable），一旦创建不能修改
 * 记录客户端的关键信息（如客户端 IP、端口、用户信息等）
 * 支持生成用于验证的签名，确保调用信息的可靠性
 * 通过 ThreadLocal 机制，支持线程级的调用上下文管理，方便在多线程环境中追踪操作来源
 * 为 HDFS 操作提供审计信息，方便问题排查、性能分析和操作溯源
 * This class is immutable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CallerContext {
  public static final Charset SIGNATURE_ENCODING = StandardCharsets.UTF_8;

  // field names
  public static final String CLIENT_IP_STR = "clientIp";//客户端 IP 的字段名。
  //客户端端口的字段名。
  public static final String CLIENT_PORT_STR = "clientPort";
  //客户端 ID 的字段名。
  public static final String CLIENT_ID_STR = "clientId";
  //客户端调用 ID 的字段名。
  public static final String CLIENT_CALL_ID_STR = "clientCallId";
  //实际执行操作的用户字段名。
  public static final String REAL_USER_STR = "realUser";
  //代理用户端口字段名，适用于代理场景。
  public static final String PROXY_USER_PORT = "proxyUserPort";
  /** The caller context.
   *
   * It will be truncated if it exceeds the maximum allowed length in
   * server. The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT}
   */
  //调用者的上下文信息，形如 key:value。
  private final String context;

  /** The caller's signature for validation.
   *
   * The signature is optional. The null or empty signature will be abandoned.
   * If the signature exceeds the maximum allowed length in server, the caller
   * context will be abandoned. The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT}
   */
  //调用者上下文的签名，用于完整性校验。
  private final byte[] signature;

  private CallerContext(Builder builder) {
    this.context = builder.getContext();
    this.signature = builder.getSignature();
  }

  public String getContext() {
    return context;
  }

  public byte[] getSignature() {
    return signature == null ?
        null : Arrays.copyOf(signature, signature.length);
  }

  @InterfaceAudience.Private
  public boolean isContextValid() {
    return context != null && !context.isEmpty();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(context).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (obj.getClass() != getClass()) {
      return false;
    } else {
      CallerContext rhs = (CallerContext) obj;
      return new EqualsBuilder()
          .append(context, rhs.context)
          .append(signature, rhs.signature)
          .isEquals();
    }
  }

  @Override
  public String toString() {
    if (!isContextValid()) {
      return "";
    }
    String str = context;
    if (signature != null) {
      str += ":";
      str += new String(signature, SIGNATURE_ENCODING);
    }
    return str;
  }

  /** The caller context builder. */
  //Builder 是用于构建 CallerContext 的静态内部类，采用建造者模式简化对象创建。
  public static final class Builder {
    public static final String KEY_VALUE_SEPARATOR = ":";
    /**
     * The illegal separators include '\t', '\n', '='.
     * User should not set illegal separator.
     */
    private static final Set<String> ILLEGAL_SEPARATORS =
        Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("\t", "\n", "=")));
    private final String fieldSeparator;
    private final StringBuilder sb = new StringBuilder();
    private byte[] signature;

    public Builder(String context) {
      this(context, HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT);
    }

    public Builder(String context, Configuration conf) {
      this(context, conf.get(HADOOP_CALLER_CONTEXT_SEPARATOR_KEY,
              HADOOP_CALLER_CONTEXT_SEPARATOR_DEFAULT));
    }

    public Builder(String context, String separator) {
      if (isValid(context)) {
        sb.append(context);
      }
      fieldSeparator = separator;
      checkFieldSeparator(fieldSeparator);
    }

    /**
     * Check whether the separator is legal.
     * The illegal separators include '\t', '\n', '='.
     * Throw IllegalArgumentException if the separator is Illegal.
     * @param separator the separator of fields.
     */
    private void checkFieldSeparator(String separator) {
      if (ILLEGAL_SEPARATORS.contains(separator)) {
        throw new IllegalArgumentException("Illegal field separator: "
            + separator);
      }
    }

    /**
     * Whether the field is valid.
     * @param field one of the fields in context.
     * @return true if the field is not null or empty.
     */
    private boolean isValid(String field) {
      return field != null && field.length() > 0;
    }

    public Builder setSignature(byte[] signature) {
      if (signature != null && signature.length > 0) {
        this.signature = Arrays.copyOf(signature, signature.length);
      }
      return this;
    }

    /**
     * Get the context.
     * For example, the context is "key1:value1,key2:value2".
     * @return the valid context or null.
     */
    public String getContext() {
      return sb.length() > 0 ? sb.toString() : null;
    }

    /**
     * Get the signature.
     * @return the signature.
     */
    public byte[] getSignature() {
      return signature;
    }

    /**
     * Append new field to the context.
     * @param field one of fields to append.
     * @return the builder.
     */
    public Builder append(String field) {
      if (isValid(field)) {
        if (sb.length() > 0) {
          sb.append(fieldSeparator);
        }
        sb.append(field);
      }
      return this;
    }

    /**
     * Append new field which contains key and value to the context.
     * @param key the key of field.
     * @param value the value of field.
     * @return the builder.
     */
    public Builder append(String key, String value) {
      if (isValid(key) && isValid(value)) {
        if (sb.length() > 0) {
          sb.append(fieldSeparator);
        }
        sb.append(key).append(KEY_VALUE_SEPARATOR).append(value);
      }
      return this;
    }

    /**
     * Append new field which contains key and value to the context
     * if the key("key:") is absent.
     * @param key the key of field.
     * @param value the value of field.
     * @return the builder.
     */
    public Builder appendIfAbsent(String key, String value) {
      if (sb.toString().contains(key + KEY_VALUE_SEPARATOR)) {
        return this;
      }
      if (isValid(key) && isValid(value)) {
        if (sb.length() > 0) {
          sb.append(fieldSeparator);
        }
        sb.append(key).append(KEY_VALUE_SEPARATOR).append(value);
      }
      return this;
    }

    public CallerContext build() {
      return new CallerContext(this);
    }
  }

  /**
   * The thread local current caller context.
   * <p>
   * Internal class for defered singleton idiom.
   * https://en.wikipedia.org/wiki/Initialization_on_demand_holder_idiom
   */
  private static final class CurrentCallerContextHolder {
    static final ThreadLocal<CallerContext> CALLER_CONTEXT =
        new InheritableThreadLocal<>();
  }

  public static CallerContext getCurrent() {
    return CurrentCallerContextHolder.CALLER_CONTEXT.get();
  }

  public static void setCurrent(CallerContext callerContext) {
    CurrentCallerContextHolder.CALLER_CONTEXT.set(callerContext);
  }
}
