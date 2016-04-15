package org.apache.storm.executor;

import com.lmax.disruptor.dsl.ProducerType;
import org.apache.storm.Config;
import org.apache.storm.cluster.*;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.metrics.SpoutThrottlingMetrics;
import org.apache.storm.executor.error.IReportError;
import org.apache.storm.executor.error.ReportErrorAndDie;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.LoadAwareCustomStreamGrouping;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ExecutorData {

    private final Map workerData;
    private final WorkerTopologyContext workerTopologyContext;
    private final List<Long> executorId;
    private final List<Integer> taskIds;
    private final String componentId;
    private volatile boolean openOrprepareWasCalled;
    private final Map stormConf;
    private final DisruptorQueue receiveQueue;
    private final String stormId;
    private final Map conf;
    private final HashMap sharedExecutorData;
    private final AtomicBoolean stormActiveAtom;
    private final AtomicReference<Map<String, DebugOptions>> stormComponentDebug;
    private final DisruptorQueue batchTransferWorkerQueue;
    private final Runnable suicideFn;
    private final IStormClusterState stormClusterState;
    private CommonStats stats;
    private final Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricToRegistry;
    private final Map<Integer, String> taskToComponent;
    private Map<String, Map<String, LoadAwareCustomStreamGrouping>> streamToComponentToGrouper;
    private final IReportError reportError;
    private ReportErrorAndDie reportErrorDie;
    private Callable<Boolean> sampler;
    private AtomicBoolean backpressure;
    private SpoutThrottlingMetrics spoutThrottlingMetrics;

    public ExecutorData(Map workerData, List<Long> executorId) {
        this.workerData = workerData;
        this.executorId = executorId;
        this.workerTopologyContext = StormCommon.makerWorkerContext(workerData);
        this.taskIds = StormCommon.executorIdToTasks(executorId);
        this.componentId = workerTopologyContext.getComponentId(taskIds.get(0));
        this.stormConf = normalizedComponentConf((Map) workerData.get("storm-conf"), workerTopologyContext, componentId);
        int sendSize = Utils.getInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        int waitTimeOutMs = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS));
        int batchSize = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE));
        int batchTimeOutMs = Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS));
        this.batchTransferWorkerQueue =
                new DisruptorQueue("executor" + executorId + "-send-queue", ProducerType.SINGLE, sendSize, waitTimeOutMs, batchSize, batchTimeOutMs);
        this.openOrprepareWasCalled = false;
        // maybe question?
        this.receiveQueue = (DisruptorQueue) (((Map) workerData.get("executor-receive-queue-map")).get("executorId"));
        this.stormId = (String) workerData.get("storm-id");
        this.conf = (Map) workerData.get("conf");
        this.sharedExecutorData = new HashMap();
        // 我现在不太确定workerData 的 storm-active-atom 从 atom 改成AtomicBoolean是否合理
        this.stormActiveAtom = (AtomicBoolean) workerData.get("storm-active-atom");
        //注意这里有可能是null值
        this.stormComponentDebug = (AtomicReference<Map<String, DebugOptions>>) workerData.get("storm-component->debug-atom");
        this.suicideFn = (Runnable)workerData.get("suicide-fn");
        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState((IStateStorage)workerData.get("state-store"), Utils.getWorkerACL(stormConf), new ClusterStateContext(DaemonType.SUPERVISOR));
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
        this.intervalToTaskToMetricToRegistry = new HashMap<>();
        this.taskToComponent = (Map<Integer, String>) workerData.get("task->component");
        this.streamToComponentToGrouper =
    }

    /**
     * Returns map of stream id to component id to grouper
     */
    private  Map<String, Map<String, LoadAwareCustomStreamGrouping>> outboundComponents(WorkerTopologyContext workerTopologyContext, String componentId, Map stormConf) {

        Map<String, Map<String, Grouping>> output_groupings =  workerTopologyContext.getTargets(componentId);

        for (Map.Entry<String, Map<String, Grouping>> entry : output_groupings.entrySet()) {

            String stream_id = entry.getKey();
            Map<String, Grouping> component_grouping = entry.getValue();

            Fields out_fields = workerTopologyContext.getComponentOutputFields(componentId, stream_id);

            Map<String, MkGrouper> componentGrouper = new HashMap<String, MkGrouper>();

            for (Map.Entry<String, Grouping> cg : component_grouping.entrySet()) {

                String component = cg.getKey();
                Grouping tgrouping = cg.getValue();

                List<Integer> outTasks = topology_context.getComponentTasks(component);
                // ATTENTION: If topology set one component parallelism as 0
                // so we don't need send tuple to it
                if (outTasks.size() > 0) {
                    MkGrouper grouper = new MkGrouper(topology_context, out_fields, tgrouping, outTasks, stream_id, workerData);
                    componentGrouper.put(component, grouper);
                }
                LOG.info("outbound_components, {}-{} for task-{} on {}", component, outTasks, topology_context.getThisTaskId(), stream_id);
            }
            if (componentGrouper.size() > 0) {
                rr.put(stream_id, componentGrouper);
            }
        }
        return rr;
    }

    private Map normalizedComponentConf(Map stormConf, WorkerTopologyContext topologyContext, String componentId) {
        List<Object> to_remove = ConfigUtils.All_CONFIGS();
        to_remove.remove(Config.TOPOLOGY_DEBUG);
        to_remove.remove(Config.TOPOLOGY_MAX_SPOUT_PENDING);
        to_remove.remove(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
        to_remove.remove(Config.TOPOLOGY_TRANSACTIONAL_ID);
        to_remove.remove(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        to_remove.remove(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS);
        to_remove.remove(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY);
        to_remove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT);
        to_remove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS);
        to_remove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT);
        to_remove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS);
        to_remove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME);
        to_remove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS);
        to_remove.remove(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME);
        to_remove.remove(Config.TOPOLOGY_STATE_PROVIDER);
        to_remove.remove(Config.TOPOLOGY_STATE_PROVIDER_CONFIG);

        Map<Object, Object> componentConf = new HashMap<Object, Object>();

        String jconf = topologyContext.getComponentCommon(componentId).get_json_conf();
        if (jconf != null) {
            componentConf = (Map<Object, Object>) JSONValue.parse(jconf);
        }
        for (Object p : to_remove) {
            componentConf.remove(p);
        }
        Map<Object, Object> ret = new HashMap<Object, Object>();
        ret.putAll(stormConf);
        ret.putAll(componentConf);
        return ret;
    }

    public String getComponentId() {
        return componentId;
    }

    public Map getStormConf() {
        return stormConf;
    }

    public String getStormId() {
        return stormId;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    public WorkerTopologyContext getWorkerTopologyContext() {
        return workerTopologyContext;
    }

    public IReportError getReportError() {
        return reportError;
    }

    public Runnable getSuicideFn() {
        return suicideFn;
    }
}
