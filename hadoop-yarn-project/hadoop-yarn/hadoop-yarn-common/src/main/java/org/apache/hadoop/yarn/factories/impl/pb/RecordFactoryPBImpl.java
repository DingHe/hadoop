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

package org.apache.hadoop.yarn.factories.impl.pb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
//主要作用是基于 Protocol Buffers (PB) 机制创建 YARN 记录 (Record) 的实例
//单例模式：使用 self 变量实现单例模式，提供 get() 方法获取唯一实例
//动态实例化：使用 Java 反射机制，根据给定的类名推导出对应的 PB 实现类，并创建其实例
//类缓存：使用 ConcurrentHashMap 缓存已解析的构造函数，提高性能，避免重复反射查找
@Private
public class RecordFactoryPBImpl implements RecordFactory {
  //用于构造 PB 实现类的包名后缀，表明 PB 相关的实现类通常位于 impl.pb 这个包下
  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb";
  //用于构造 PB 实现类的类名后缀，PB 版本的类通常以 PBImpl 结尾
  private static final String PB_IMPL_CLASS_SUFFIX = "PBImpl";

  private static final RecordFactoryPBImpl self = new RecordFactoryPBImpl();
  private Configuration localConf = new Configuration();
  //缓存类的无参构造方法，以避免重复反射查找，提高实例化效率
  private ConcurrentMap<Class<?>, Constructor<?>> cache = new ConcurrentHashMap<Class<?>, Constructor<?>>();

  private RecordFactoryPBImpl() {
  }
  
  public static RecordFactory get() {
    return self;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <T> T newRecordInstance(Class<T> clazz) {
    
    Constructor<?> constructor = cache.get(clazz);
    if (constructor == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(clazz));
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException("Failed to load class: ["
            + getPBImplClassName(clazz) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor();
        constructor.setAccessible(true);
        cache.putIfAbsent(clazz, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnRuntimeException("Could not find 0 argument constructor", e);
      }
    }
    try {
      Object retObject = constructor.newInstance();
      return (T)retObject;
    } catch (InvocationTargetException e) {
      throw new YarnRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new YarnRuntimeException(e);
    } catch (InstantiationException e) {
      throw new YarnRuntimeException(e);
    }
  }
  //根据 clazz 计算出对应的 PB 实现类的全限定类名
  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }
  //获取 clazz 的简单类名（不包含包路径）
  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }
  //获取 clazz 的包名
  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }
}
