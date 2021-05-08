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

package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Region Server Quota Manager.
 * It is responsible to provide access to the quota information of each user/table.
 *
 * The direct user of this class is the RegionServer that will get and check the
 * user/table quota for each operation (put, get, scan).
 * For system tables and user/table with a quota specified, the quota check will be a noop.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerCompactionOffloadManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerCompactionOffloadManager.class);

  private final RegionServerServices rsServices;
  
  private volatile boolean compactionOffloadEnabled;
  // Storage for quota rpc throttle
  private CompactionOffloadSwitchStorage compactionOffloadSwitchStorage;

  public RegionServerCompactionOffloadManager(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    compactionOffloadSwitchStorage =
        new CompactionOffloadSwitchStorage(rsServices.getZooKeeper(), rsServices.getConfiguration());
  }

  public void start(final RpcScheduler rpcScheduler) throws IOException {

    LOG.info("Initializing RPC quota support");

    // Initialize quota cache
    compactionOffloadEnabled = compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
    LOG.info("Start rpc quota manager and rpc throttle enabled is {}", compactionOffloadEnabled);
  }

  public void stop() {

  }

  protected boolean isCompactionOffloadEnabled() {
    return compactionOffloadEnabled;
  }
  
  public void switchCompactionOffload(boolean enable) throws IOException {
    if (compactionOffloadEnabled != enable) {
      boolean previousEnabled = compactionOffloadEnabled;
      compactionOffloadEnabled = compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
      LOG.info("Switch rpc throttle from {} to {}", previousEnabled, compactionOffloadEnabled);
    } else {
      LOG.warn("Skip switch rpc throttle because previous value {} is the same as current value {}",
        compactionOffloadEnabled, enable);
    }
  }

}
