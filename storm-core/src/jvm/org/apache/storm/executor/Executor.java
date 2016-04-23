package org.apache.storm.executor;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.StormTimer;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.BuiltinBoltMetrics;
import org.apache.storm.daemon.metrics.BuiltinSpoutMetrics;
import org.apache.storm.executor.bolt.BoltExecutor;
import org.apache.storm.executor.spout.SpoutExecutor;
import org.apache.storm.spout.ISpout;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.stats.StatsUtil;
import org.apache.storm.task.IBolt;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DisruptorBackpressureCallback;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.WorkerBackpressureThread;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class Executor {

    private static Logger LOG = Logger.getLogger(Executor.class);
    private final Map workerData;
    private final List<Long> executorId;
    private final Map<String, String> credentials;
    private final ExecutorData executorData;
    private final String componentId;
    private final Map stormConf;

    private Executor(Map workerData, List<Long> executorId, Map<String, String> credentials) {
        this.workerData = workerData;
        this.executorId = executorId;
        this.credentials = credentials;
        this.executorData = new ExecutorData(workerData, executorId);
        this.componentId = executorData.getComponentId();
        this.stormConf = executorData.getStormConf();
        LOG.info("Loading executor " + componentId + ":" + executorId);
    }

    private ExecutorShutdown execute() throws Exception {
        Map<Integer, Task> taskDatas = new HashMap<>();
        for (Integer t : executorData.getTaskIds()) {
            Task task = new Task(executorData, t);
            ExecutorCommon.sendUnanchored(task, StormCommon.SYSTEM_STREAM_ID, new Values("startup"), executorData.getExecutorTransfer());
            taskDatas.put(t, task);
        }
        LOG.info("Loading executor tasks " + componentId + ":" + executorId);
        registerBackpressure();
        Utils.SmartThread systemThreads =
                Utils.asyncLoop(executorData.getExecutorTransfer(), executorData.getExecutorTransfer().getName(), executorData.getReportErrorDie());
        BaseExecutor baseExecutor = null;

        String type = executorData.getType();
        if (StatsUtil.SPOUT.equals(type)) {
            baseExecutor = new SpoutExecutor(executorData, taskDatas, credentials);
        } else if (StatsUtil.BOLT.equals(type)) {
            baseExecutor = new BoltExecutor(executorData, taskDatas, credentials);
        } else {
            throw new RuntimeException("Could not find  " + componentId + " in topology");
        }

        String handlerName = componentId + "-executor" + executorId;
        Utils.SmartThread handlers = Utils.asyncLoop(baseExecutor, false, executorData.getReportErrorDie(), Thread.NORM_PRIORITY, true, true, handlerName);
        setupTicks(StatsUtil.SPOUT.equals(type));
        LOG.info("Finished loading executor " + componentId + ":" + executorId);
        List<Utils.SmartThread> threads = new ArrayList<>();
        return new ExecutorShutdown(executorData, Lists.newArrayList(systemThreads, handlers), taskDatas);
    }

    private void registerBackpressure() {
        DisruptorQueue receiveQueue = executorData.getReceiveQueue();
        receiveQueue.registerBackpressureCallback(new DisruptorBackpressureCallback() {
            @Override
            public void highWaterMark() throws Exception {
                AtomicBoolean enablePressure = executorData.getBackpressure();
                if (!enablePressure.get()) {
                    enablePressure.set(true);
                    LOG.debug("executor " + executorId + " is congested, set backpressure flag true");
                    WorkerBackpressureThread.notifyBackpressureChecker(workerData.get("backpressure-trigger"));
                }
            }

            @Override
            public void lowWaterMark() throws Exception {
                AtomicBoolean enablePressure = executorData.getBackpressure();
                if (enablePressure.get()) {
                    enablePressure.set(false);
                    LOG.debug("executor " + executorId + " is not-congested, set backpressure flag false");
                    WorkerBackpressureThread.notifyBackpressureChecker(workerData.get("backpressure-trigger"));
                }
            }
        });
        receiveQueue.setHighWaterMark(Utils.getDouble(stormConf.get(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK)));
        receiveQueue.setLowWaterMark(Utils.getDouble(stormConf.get(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK)));
        receiveQueue.setEnableBackpressure(Utils.getBoolean(stormConf.get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false));
    }

    protected void setupTicks(boolean isSpout) {
        final Integer tickTimeSecs = Utils.getInt(stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS), null);
        if (tickTimeSecs != null) {
            if (Utils.isSystemId(componentId) || ((stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) == false) && isSpout)) {
                LOG.info("Timeouts disabled for executor " + componentId + ":" + executorId);
                StormTimer timerTask = (StormTimer) workerData.get("user-timer");
                timerTask.scheduleRecurring(tickTimeSecs, tickTimeSecs, new Runnable() {
                    @Override
                    public void run() {
                        TupleImpl tuple = new TupleImpl(executorData.getWorkerTopologyContext(), new Values(tickTimeSecs), (int) Constants.SYSTEM_TASK_ID,
                                Constants.METRICS_TICK_STREAM_ID);
                        AddressedTuple addressedTuple = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple);
                        executorData.getReceiveQueue().publish(addressedTuple);
                    }
                });
            }
        }
    }

    public static ExecutorShutdown mkExecutor(Map workerData, List<Long> executorId, Map<String, String> credentials) throws Exception {
        Executor executor = new Executor(workerData, executorId, credentials);
        return executor.execute();
    }

}
