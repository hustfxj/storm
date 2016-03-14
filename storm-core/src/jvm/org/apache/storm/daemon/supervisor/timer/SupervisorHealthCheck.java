/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.supervisor.timer;

import org.apache.storm.command.HealthCheck;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.supervisor.workermanager.IWorkerManager;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class SupervisorHealthCheck implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorHealthCheck.class);

    private SupervisorData supervisorData;

    public SupervisorHealthCheck(SupervisorData supervisorData) {
        this.supervisorData = supervisorData;
    }

    @Override
    public void run() {
        Map conf = supervisorData.getConf();
        IWorkerManager workerManager = supervisorData.getWorkerManager();
        int healthCode = HealthCheck.healthCheck(conf);
        Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);
        if (healthCode != 0) {
            for (String workerId : workerIds) {
                try {
                    workerManager.shutdownWorker(supervisorData.getSupervisorId(), workerId, supervisorData.getWorkerThreadPids());
                    boolean success = workerManager.cleanupWorker(workerId);
                    if (success){
                        supervisorData.getDeadWorkers().remove(workerId);
                    }
                } catch (Exception e) {
                    throw Utils.wrapInRuntime(e);
                }
            }
        }
    }
}
