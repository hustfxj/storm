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
package org.apache.storm.daemon.supervisor;

import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.container.cgroup.CgroupManager;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.messaging.IContext;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SupervisorData {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorData.class);

    private Map conf;
    private IContext sharedContext;
    private volatile boolean active;
    private ISupervisor iSupervisor;
    private Utils.UptimeComputer upTime;
    private String stormVersion;

    private ConcurrentHashMap<String, String> workerThreadPidsAtom; // for local mode

    private IStormClusterState stormClusterState;

    private LocalState localState;

    private String supervisorId;

    private String assignmentId;

    private String hostName;

    // used for reporting used ports when heartbeating
    private ConcurrentHashMap<Long, LocalAssignment> currAssignment;

    private StormTimer heartbeatTimer;

    private StormTimer eventTimer;

    private StormTimer blobUpdateTimer;

    private Localizer localizer;

    private ConcurrentHashMap<String, Map<String, Object>> assignmentVersions;

    private AtomicInteger syncRetry;

    private final Object downloadLock = new Object();

    private ConcurrentHashMap<String, List<ProfileRequest>> stormIdToProfileActions;

    private CgroupManager resourceIsolationManager;

    private ConcurrentHashSet<String> deadWorkers;

    public SupervisorData(Map conf, IContext sharedContext, ISupervisor iSupervisor) {
        this.conf = conf;
        this.sharedContext = sharedContext;
        this.iSupervisor = iSupervisor;
        this.active = true;
        this.upTime = Utils.makeUptimeComputer();
        this.stormVersion = VersionInfo.getVersion();
        this.workerThreadPidsAtom = new ConcurrentHashMap<String, String>();
        this.deadWorkers = new ConcurrentHashSet();

        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = SupervisorUtils.supervisorZkAcls();
        }

        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.SUPERVISOR));
        } catch (Exception e) {
            LOG.error("supervisor can't create stormClusterState");
            throw Utils.wrapInRuntime(e);
        }

        try {
            this.localState = ConfigUtils.supervisorState(conf);
            this.localizer = Utils.createLocalizer(conf, ConfigUtils.supervisorLocalDir(conf));
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
        this.supervisorId = iSupervisor.getSupervisorId();
        this.assignmentId = iSupervisor.getAssignmentId();

        try {
            this.hostName = Utils.hostname(conf);
        } catch (UnknownHostException e) {
            throw Utils.wrapInRuntime(e);
        }

        this.currAssignment = new ConcurrentHashMap<>();

        this.heartbeatTimer = new StormTimer(null, new DefaultUncaughtExceptionHandler());

        this.eventTimer = new StormTimer(null, new DefaultUncaughtExceptionHandler());

        this.blobUpdateTimer = new StormTimer("blob-update-timer", new DefaultUncaughtExceptionHandler());

        this.assignmentVersions = new ConcurrentHashMap<>();
        this.syncRetry = new AtomicInteger(0);
        this.stormIdToProfileActions = new ConcurrentHashMap<>();
        if (Utils.getBoolean(conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE), false)) {
            try {
                this.resourceIsolationManager = (CgroupManager) Utils.newInstance((String) conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN));
                this.resourceIsolationManager.prepare(conf);
                LOG.info("Using resource isolation plugin {} {}", conf.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN), resourceIsolationManager);
            } catch (IOException e) {
                throw Utils.wrapInRuntime(e);
            }
        } else {
            this.resourceIsolationManager = null;
        }
    }

    public ConcurrentHashMap<String, List<ProfileRequest>> getStormIdToProfileActions() {
        return stormIdToProfileActions;
    }

    public void setStormIdToProfileActions(Map<String, List<ProfileRequest>> stormIdToProfileActions) {
        this.stormIdToProfileActions.clear();
        this.stormIdToProfileActions.putAll(stormIdToProfileActions);
    }

    public Map getConf() {
        return conf;
    }

    public void setConf(Map conf) {
        this.conf = conf;
    }

    public IContext getSharedContext() {
        return sharedContext;
    }

    public void setSharedContext(IContext sharedContext) {
        this.sharedContext = sharedContext;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public ISupervisor getiSupervisor() {
        return iSupervisor;
    }

    public void setiSupervisor(ISupervisor iSupervisor) {
        this.iSupervisor = iSupervisor;
    }

    public Utils.UptimeComputer getUpTime() {
        return upTime;
    }

    public void setUpTime(Utils.UptimeComputer upTime) {
        this.upTime = upTime;
    }

    public String getStormVersion() {
        return stormVersion;
    }

    public void setStormVersion(String stormVersion) {
        this.stormVersion = stormVersion;
    }

    public ConcurrentHashMap<String, String> getWorkerThreadPidsAtom() {
        return workerThreadPidsAtom;
    }

    public void setWorkerThreadPidsAtom(ConcurrentHashMap<String, String> workerThreadPidsAtom) {
        this.workerThreadPidsAtom = workerThreadPidsAtom;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public void setStormClusterState(IStormClusterState stormClusterState) {
        this.stormClusterState = stormClusterState;
    }

    public LocalState getLocalState() {
        return localState;
    }

    public void setLocalState(LocalState localState) {
        this.localState = localState;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public void setSupervisorId(String supervisorId) {
        this.supervisorId = supervisorId;
    }

    public String getAssignmentId() {
        return assignmentId;
    }

    public void setAssignmentId(String assignmentId) {
        this.assignmentId = assignmentId;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public ConcurrentHashMap<Long, LocalAssignment> getCurrAssignment() {
        return currAssignment;
    }

    public void setCurrAssignment(Map<Long, LocalAssignment> currAssignment) {
        this.currAssignment.clear();
        this.currAssignment.putAll(currAssignment);
    }

    public StormTimer getHeartbeatTimer() {
        return heartbeatTimer;
    }

    public void setHeartbeatTimer(StormTimer heartbeatTimer) {
        this.heartbeatTimer = heartbeatTimer;
    }

    public StormTimer getEventTimer() {
        return eventTimer;
    }

    public void setEventTimer(StormTimer eventTimer) {
        this.eventTimer = eventTimer;
    }

    public StormTimer getBlobUpdateTimer() {
        return blobUpdateTimer;
    }

    public void setBlobUpdateTimer(StormTimer blobUpdateTimer) {
        this.blobUpdateTimer = blobUpdateTimer;
    }

    public Localizer getLocalizer() {
        return localizer;
    }

    public void setLocalizer(Localizer localizer) {
        this.localizer = localizer;
    }

    public AtomicInteger getSyncRetry() {
        return syncRetry;
    }

    public void setSyncRetry(AtomicInteger syncRetry) {
        this.syncRetry = syncRetry;
    }

    public ConcurrentHashMap<String, Map<String, Object>> getAssignmentVersions() {
        return assignmentVersions;
    }

    public void setAssignmentVersions(Map<String, Map<String, Object>> assignmentVersions) {
        this.assignmentVersions.clear();
        this.assignmentVersions.putAll(assignmentVersions);
    }

    public CgroupManager getResourceIsolationManager() {
        return resourceIsolationManager;
    }

    public void setResourceIsolationManager(CgroupManager resourceIsolationManager) {
        this.resourceIsolationManager = resourceIsolationManager;
    }

    public Object getDownloadLock() {
        return downloadLock;
    }

    public ConcurrentHashSet getDeadWorkers() {
        return deadWorkers;
    }

    public void setDeadWorkers(ConcurrentHashSet deadWorkers) {
        this.deadWorkers = deadWorkers;
    }
}