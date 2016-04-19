package org.apache.storm.executor;

import org.apache.storm.Config;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.Task;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ExecutorCommon {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorCommon.class);

    public static void sendUnanchored(Task taskData, String stream, List<Object> values, ExecutorTransfer transfer) {

        Tuple tuple = taskData.getTuple(stream, values);
        java.util.List<Integer> tasks = taskData.getOutgoingTasks(stream, values);
        if (tasks.size() == 0) {
            return;
        }
        for (Integer t : tasks) {
            transfer.transfer(t, tuple);
        }
    }

    /**
     * Send sampled data to the eventlogger if the global or component level debug flag is set (via nimbus api).
     * 
     * @param executorData
     * @param taskData
     * @param values
     * @param componentId
     * @param messageId
     * @param random
     */
    public static void sendToEventLogger(ExecutorData executorData, Task taskData, List values, String componentId, Object messageId, Random random) {
        Map<String, DebugOptions> componentDebug = executorData.getStormComponentDebug().get();
        DebugOptions debugOptions = componentDebug.get(componentId);
        if (debugOptions == null) {
            debugOptions = componentDebug.get(executorData.getStormId());
        }
        double spct = ((debugOptions != null) && (debugOptions.is_enable())) ? debugOptions.get_samplingpct() : 0;
        if (spct > 0 && (random.nextDouble() * 100) < spct) {
            sendUnanchored(taskData, StormCommon.EVENTLOGGER_STREAM_ID, new Values(componentId, messageId, System.currentTimeMillis(), values),
                    executorData.getExecutorTransfer());
        }
    }

    public static void ackSpoutMsg(ExecutorData executorData, Task taskData, TupleInfo tupleInfo) {
        try {
            Map stormConf = executorData.getStormConf();
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false)) {
                LOG.info("SPOUT Acking message {} {}", tupleInfo.getId(), tupleInfo.getMessageId());
            }
            spout.ack(tupleInfo.getMessageId());
            new SpoutAckInfo(tupleInfo.getMessageId(), taskId, tupleInfo.getTimestamp()).applyOn(taskData.getUserContext());
            if (tupleInfo.getTimestamp() != 0)
                ((SpoutExecutorStats) executorData.getStats()).spoutAckedTuple(tupleInfo.getStream(), tupleInfo.getTimestamp());

        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }

    public static void failSpoutMsg(ExecutorData executorData, Task taskData, TupleInfo tupleInfo, String reason) {
        try {
            Map stormConf = executorData.getStormConf();
            ISpout spout = (ISpout) taskData.getTaskObject();
            int taskId = taskData.getTaskId();
            if (Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false)) {
                LOG.info("SPOUT Failing {} : {} REASON: {}", tupleInfo.getId(), tupleInfo, reason);
            }
            spout.fail(tupleInfo.getMessageId());
            new SpoutFailInfo(tupleInfo.getMessageId(), taskId, tupleInfo.getTimestamp()).applyOn(taskData.getUserContext());
            if (tupleInfo.getTimestamp() != 0)
                ((SpoutExecutorStats) executorData.getStats()).spoutFailedTuple(tupleInfo.getStream(), tupleInfo.getTimestamp());

        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
