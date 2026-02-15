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

package org.apache.hadoop.yarn.ams;

/**
 * This is a marker interface for a context object that is injected into
 * the ApplicationMasterService processor. The processor implementation
 * is free to type cast this based on the availability of the context's
 * implementation in the classpath.
 */
//标记接口（Marker Interface），它用于在 ApplicationMasterService 处理器（Processor）中注入上下文对象。
//标记接口的特点是不包含任何方法，仅用于标识一个类属于特定的类型
//主要作用：
//用于依赖注入（DI），ApplicationMasterService 可以接收实现该接口的对象，而不关心具体的实现细节
public interface ApplicationMasterServiceContext {

}
