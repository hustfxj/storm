package org.apache.storm.executor.spout;

import org.apache.logging.log4j.EventLogger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.daemon.metrics.SpoutThrottlingMetrics;
import org.apache.storm.executor.BaseExecutor;
import org.apache.storm.executor.ExecutorCommon;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.ISpoutWaitStrategy;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class SpoutExecutor extends BaseExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SpoutExecutor.class);

    private final ISpoutWaitStrategy spoutWaitStrategy;
    private final Integer maxSpoutPending;
    private final AtomicBoolean lastActive;
    private final List<ISpout> spouts;
    private final ISpoutOutputCollector outputCollector;
    private final MutableLong emittedCount; // 共用
    private final MutableLong emptyEmitStreak; // 共用
    private final SpoutThrottlingMetrics spoutThrottlingMetrics; // 共用
    private final boolean isAcker; // 共用
    private final Random random; // 共用
    private final EventLogger isEventLoggers; // 共用
    private final RotatingMap<Long, TupleInfo> pending; // 共用


    public SpoutExecutor(ExecutorData executorData, Map<Integer, Task> taskDatas, Map<String, String> credentials) {
        super(executorData, taskDatas, credentials);
        this.spoutWaitStrategy = (ISpoutWaitStrategy)Utils.newInstance((String)stormConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY));
        this.spoutWaitStrategy.prepare(stormConf);
        this.maxSpoutPending = Utils.getInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)) * taskDatas.size();
        this.lastActive = new AtomicBoolean(false);
        this.spouts = new ArrayList<>();
        for (Task task : taskDatas.values())





    }

    @Override
    public Object call() throws Exception {
        return null;
    }

    @Override
    public void tupleActionFn(int taskId, TupleImpl tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId == Constants.SYSTEM_TICK_STREAM_ID)
            pending.rotate();
        else if (streamId == Constants.METRICS_TICK_STREAM_ID) {
            metricsTick(taskDatas.get(taskId), tuple);
        } else if (streamId == Constants.CREDENTIALS_CHANGED_STREAM_ID) {
            Object object = taskDatas.get(taskId).getTaskObject();
            if (object instanceof ICredentialsListener) {
                ((ICredentialsListener) object).setCredentials((Map<String, String>) tuple.getValue(0));
            }
        } else if (streamId == Acker.ACKER_RESET_TIMEOUT_STREAM_ID) {
            Long id = (Long) tuple.getValue(0);
            TupleInfo pendingForId = pending.get(id);
            if (pendingForId != null)
                pending.put(id, pendingForId);
        } else {
            Long id = (Long) tuple.getValue(0);
            TupleInfo tupleInfo = (TupleInfo) pending.remove(id);
            if (tupleInfo.getMessageId() != null) {
                if (taskId != tupleInfo.getTaskId()) {
                    throw new RuntimeException("Fatal error, mismatched task ids: " + taskId + " " + tupleInfo.getTaskId());
                }
                long startTimeMs = tupleInfo.getTimestamp();
                long timeDelta = 0;
                if (startTimeMs != 0)
                     timeDelta = Time.deltaMs(tupleInfo.getTimestamp());
                if (tupleInfo.getStream() == Acker.ACKER_ACK_STREAM_ID){
                    ExecutorCommon.ackSpoutMsg(executorData, taskDatas.get(taskId), tupleInfo);
                }else if (tupleInfo.getStream() == Acker.ACKER_FAIL_STREAM_ID){
                    ExecutorCommon.failSpoutMsg(executorData, taskDatas.get(taskId), tupleInfo, "FAIL-STREAM");
                }

            }

        }

    }
}
