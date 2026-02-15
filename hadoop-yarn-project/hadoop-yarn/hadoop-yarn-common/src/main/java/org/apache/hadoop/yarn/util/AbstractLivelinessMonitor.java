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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.AbstractService;

/**
 * A simple liveliness monitor with which clients can register, trust the
 * component to monitor liveliness, get a call-back on expiry and then finally
 * unregister.
 */
//一个简单的活跃性监控器，客户端可以注册对象，并由该监控器定期检查对象的活跃状态。
// 如果对象在规定的时间内未收到心跳信号，则会被认为失效，调用过期处理方法并将其从监控列表中移除。
// 该类通过使用一个定时检查线程，定期检查所有注册对象的心跳状态
@Public
@Evolving
public abstract class AbstractLivelinessMonitor<O> extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLivelinessMonitor.class);

  //thread which runs periodically to see the last time since a heartbeat is
  //received.
  //定期检查注册对象心跳的线程。如果有对象超时未收到心跳，则触发过期处理
  private Thread checkerThread;
  //标记监控器是否已经停止。用于控制线程的终止
  private volatile boolean stopped;
  //默认过期时间（5分钟，单位：毫秒）。如果一个对象在这个时间内未收到心跳，则认为其已过期
  public static final int DEFAULT_EXPIRE = 5*60*1000;//5 mins
  //对象的过期时间间隔。默认值为DEFAULT_EXPIRE
  private long expireInterval = DEFAULT_EXPIRE;
  //检查心跳状态的间隔时间。默认是过期时间的三分之一
  private long monitorInterval = expireInterval / 3;
  //控制在监控器启动时是否重置所有注册对象的计时器
  private volatile boolean resetTimerOnStart = true;
  //用于获取当前时间的时钟。默认为MonotonicClock，也可以通过构造函数注入自定义时钟
  private final Clock clock;
  //一个保存所有已注册对象及其最后更新时间的映射。键为对象，值为该对象的最后更新时间戳
  private Map<O, Long> running = new HashMap<O, Long>();

  public AbstractLivelinessMonitor(String name, Clock clock) {
    super(name);
    this.clock = clock;
  }

  public AbstractLivelinessMonitor(String name) {
    this(name, new MonotonicClock());
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    resetTimer();
    checkerThread = new Thread(new PingChecker());
    checkerThread.setName("Ping Checker for "+getName());
    checkerThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (checkerThread != null) {
      checkerThread.interrupt();
    }
    super.serviceStop();
  }
  //定义当某个对象超时过期时的处理逻辑
  protected abstract void expire(O ob);

  protected void setExpireInterval(long expireInterval) {
    this.expireInterval = expireInterval;
  }

  protected long getExpireInterval(O o) {
    // by-default return for all the registered object interval.
    return this.expireInterval;
  }

  protected void setMonitorInterval(long monitorInterval) {
    this.monitorInterval = monitorInterval;
  }
  //当接收到某个对象的心跳时调用该方法
  public synchronized void receivedPing(O ob) {
    //only put for the registered objects
    if (running.containsKey(ob)) {
      //如果该对象已经注册，则更新其最后心跳的时间戳
      running.put(ob, clock.getTime());
    }
  }
  //注册一个对象，并设置其过期时间为当前时间
  public synchronized void register(O ob) {
    register(ob, clock.getTime());
  }
  //注册一个对象，并设置其过期时间为当前时间
  public synchronized void register(O ob, long expireTime) {
    running.put(ob, expireTime);
  }

  public synchronized void unregister(O ob) {
    running.remove(ob);
  }

  public synchronized void resetTimer() {
    if (resetTimerOnStart) {
      long time = clock.getTime();
      for (O ob : running.keySet()) {
        running.put(ob, time);
      }
    }
  }

  protected void setResetTimeOnStart(boolean resetTimeOnStart) {
    this.resetTimerOnStart = resetTimeOnStart;
  }

  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        synchronized (AbstractLivelinessMonitor.this) {
          Iterator<Map.Entry<O, Long>> iterator = running.entrySet().iterator();

          // avoid calculating current time everytime in loop
          long currentTime = clock.getTime();

          while (iterator.hasNext()) {
            Map.Entry<O, Long> entry = iterator.next();
            O key = entry.getKey();
            long interval = getExpireInterval(key);
            if (currentTime > entry.getValue() + interval) {
              iterator.remove();
              expire(key);
              LOG.info("Expired:" + entry.getKey().toString()
                  + " Timed out after " + interval / 1000 + " secs");
            }
          }
        }
        try {
          Thread.sleep(monitorInterval);
        } catch (InterruptedException e) {
          LOG.info(getName() + " thread interrupted");
          break;
        }
      }
    }
  }

}
