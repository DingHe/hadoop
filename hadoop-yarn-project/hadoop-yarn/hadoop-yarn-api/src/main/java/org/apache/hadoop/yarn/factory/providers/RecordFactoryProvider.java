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

package org.apache.hadoop.yarn.factory.providers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
//主要用于提供 RecordFactory 的实例，该工厂用于创建 YARN 记录（Record），这些记录通常用于 YARN 内部的数据结构和通信机制
//动态加载：RecordFactoryProvider 通过 Java 反射机制动态加载 RecordFactory 实现类，并调用其 get() 方法获取实例
//默认配置支持：如果用户未提供 Configuration，则使用默认的 Configuration 进行工厂类的解析
//解耦设计：通过配置文件指定 RecordFactory 实现，避免直接依赖具体实现，提高了灵活性
@LimitedPrivate({ "MapReduce", "YARN" })
@Unstable
public class RecordFactoryProvider {
  private static Configuration defaultConf;
  
  static {
    defaultConf = new Configuration();
  }
  
  private RecordFactoryProvider() {
  }
  //获取 RecordFactory 实例，支持从 Configuration 中读取 RecordFactory 实现类的配置，并通过反射获取其实例
  public static RecordFactory getRecordFactory(Configuration conf) {
    if (conf == null) {
      //Assuming the default configuration has the correct factories set.
      //Users can specify a particular factory by providing a configuration.
      conf = defaultConf;
    }
    //读取 YarnConfiguration.IPC_RECORD_FACTORY_CLASS 配置，获取 RecordFactory 具体实现类的类名
    String recordFactoryClassName = conf.get(
        YarnConfiguration.IPC_RECORD_FACTORY_CLASS,
        YarnConfiguration.DEFAULT_IPC_RECORD_FACTORY_CLASS);
    return (RecordFactory) getFactoryClassInstance(recordFactoryClassName);
  }
  
  private static Object getFactoryClassInstance(String factoryClassName) {
    try {
      Class<?> clazz = Class.forName(factoryClassName);
      Method method = clazz.getMethod("get", null);
      method.setAccessible(true);
      return method.invoke(null, null);
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnRuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    }
  }
}
