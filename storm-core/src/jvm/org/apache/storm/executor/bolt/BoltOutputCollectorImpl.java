package org.apache.storm.executor.bolt;

import org.apache.storm.Config;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.ExecutorCommon;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class BoltOutputCollectorImpl implements IOutputCollector {

    private static final Logger LOG = LoggerFactory.getLogger(BoltOutputCollectorImpl.class);

    private final ExecutorData executorData;
    private final Task taskData;
    private final int taskId;
    private final Random random; // 共用
    private final Boolean isEventLoggers; // 共用

    public BoltOutputCollectorImpl(ExecutorData executorData, Task taskData, int taskId, Random random, Boolean isEventLoggers) {
        this.executorData = executorData;
        this.taskData = taskData;
        this.taskId = taskId;
        this.random = random;
        this.isEventLoggers = isEventLoggers;
    }

    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return boltEmit(streamId, anchors, tuple, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        boltEmit(streamId, anchors, tuple, taskId);
    }

    private List<Integer> boltEmit(String streamId, Collection<Tuple> anchors, List<Object> values, Integer targetTaskId) {

        List<Integer> outTasks = null;
        if (targetTaskId != null) {
            outTasks = taskData.getOutgoingTasks(targetTaskId, streamId, values);
        } else {
            outTasks = taskData.getOutgoingTasks(streamId, values);
        }

        for (Integer t : outTasks) {
            Map<Long, Long> anchorsToids = new HashMap<Long, Long>();
            if (anchors != null) {
                for (Tuple a : anchors) {
                    // Long edge_id = MessageId.generateId();
                    Long edgeId = MessageId.generateId(random);
                    ((TupleImpl) a).updateAckVal(edgeId);
                    for (Long root_id : a.getMessageId().getAnchorsToIds().keySet()) {
                        putXor(anchorsToids, root_id, edgeId);
                    }
                }
            }
            MessageId msgid = MessageId.makeId(anchorsToids);
            TupleImpl tupleExt = new TupleImpl(executorData.getWorkerTopologyContext(), values, taskId, streamId, msgid);
            executorData.getExecutorTransfer().transfer(t, tupleExt);
        }
        if (isEventLoggers)
            ExecutorCommon.sendToEventLogger(executorData, taskData, values, executorData.getComponentId(), null, random);
        if (outTasks == null)
            outTasks = new ArrayList<>();
        return outTasks;
    }

    @Override
    public void ack(Tuple input) {
        long ackvalue = ((TupleImpl) input).getAckVal();
        Map<Long, Long> anchorsToIds = input.getMessageId().getAnchorsToIds();
        for (Map.Entry<Long, Long> entry : anchorsToIds.entrySet()) {
            ExecutorCommon.sendUnanchored(taskData, Acker.ACKER_ACK_STREAM_ID, new Values(entry.getKey(), Utils.bitXor(entry.getValue(), ackvalue)),
                    executorData.getExecutorTransfer());
        }
        long delta = tupleTimeDelta((TupleImpl) input);
        boolean isDebug = Utils.getBoolean(executorData.getStormConf().get(Config.TOPOLOGY_DEBUG), false);
        if (isDebug) {
            LOG.info("BOLT ack TASK: {} TIME: {} TUPLE: {}", taskId, delta, input);
        }
        BoltAckInfo boltAckInfo = new BoltAckInfo(input, taskId, delta);
        boltAckInfo.applyOn(taskData.getUserContext());
        if (delta != 0) {
            ((BoltExecutorStats) executorData.getStats()).boltAckedTuple(input.getSourceComponent(), input.getSourceStreamId(), delta);
        }
    }

    @Override
    public void fail(Tuple input) {
        Set<Long> roots = input.getMessageId().getAnchors();
        for (Long root : roots) {
            ExecutorCommon.sendUnanchored(taskData, Acker.ACKER_FAIL_STREAM_ID, new Values(root), executorData.getExecutorTransfer());
        }
        long delta = tupleTimeDelta((TupleImpl) input);
        boolean isDebug = Utils.getBoolean(executorData.getStormConf().get(Config.TOPOLOGY_DEBUG), false);
        if (isDebug) {
            LOG.info("BOLT fail TASK: {} TIME: {} TUPLE: {}", taskId, delta, input);
        }
        BoltFailInfo boltFailInfo = new BoltFailInfo(input, taskId, delta);
        boltFailInfo.applyOn(taskData.getUserContext());
        if (delta != 0) {
            ((BoltExecutorStats) executorData.getStats()).boltFailedTuple(input.getSourceComponent(), input.getSourceStreamId(), delta);
        }
    }

    @Override
    public void resetTimeout(Tuple input) {
        Set<Long> roots = input.getMessageId().getAnchors();
        for (Long root : roots) {
            ExecutorCommon.sendUnanchored(taskData, Acker.ACKER_RESET_TIMEOUT_STREAM_ID, new Values(root), executorData.getExecutorTransfer());
        }
    }

    @Override
    public void reportError(Throwable error) {
        executorData.getReportError().report(error);
    }

    private long tupleTimeDelta(TupleImpl tuple) {
        Long ms = tuple.getProcessSampleStartTime();
        if (ms != null)
            return Time.deltaMs(ms);
        return 0;
    }

    private void putXor(Map<Long, Long> pending, Long key, Long id) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = Long.valueOf(0);
        }
        pending.put(key, Utils.bitXor(curr, id));
    }
}
