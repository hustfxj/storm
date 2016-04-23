package org.apache.storm.executor;

import org.apache.storm.Constants;
import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.daemon.Task;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.spout.ISpout;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ExecutorShutdown implements Shutdownable, IRunningExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorShutdown.class);
    private final ExecutorData executorData;
    private final List<Utils.SmartThread> threads;
    private final Map<Integer, Task> taskDatas;

    public ExecutorShutdown(ExecutorData executorData, List<Utils.SmartThread> threads, Map<Integer, Task> taskDatas) {
        this.executorData = executorData;
        this.threads = threads;
        this.taskDatas = taskDatas;
    }

    @Override
    public ExecutorStats renderStats() {
        return executorData.getStats().renderStats();
    }

    @Override
    public List<Long> getExecutorId() {
        return executorData.getExecutorId();
    }

    @Override
    public void credenetialsChanged(Credentials credentials) {
        TupleImpl tuple = new TupleImpl(executorData.getWorkerTopologyContext(), new Values(credentials), (int) Constants.SYSTEM_TASK_ID,
                Constants.CREDENTIALS_CHANGED_STREAM_ID);
        AddressedTuple addressedTuple = new AddressedTuple(AddressedTuple.BROADCAST_DEST, tuple);
        executorData.getReceiveQueue().publish(addressedTuple);
    }

    @Override
    public boolean getBackPressureFlag() {
        return executorData.getBackpressure().get();
    }

    @Override
    public void shutdown() {
        try {
            LOG.info("Shutting down executor " + executorData.getComponentId() + ":" + executorData.getExecutorId());
            executorData.getReceiveQueue().haltWithInterrupt();
            executorData.getBatchTransferWorkerQueue().haltWithInterrupt();
            for (Utils.SmartThread t : threads) {
                t.interrupt();
                t.join();
            }
            executorData.getStats().cleanupStats();
            for (Task task : taskDatas.values()) {
                TopologyContext userContext = task.getUserContext();
                for (ITaskHook hook : userContext.getHooks()) {
                    hook.cleanup();
                }
            }
            executorData.getStormClusterState().disconnect();
            if (executorData.getOpenOrprepareWasCalled().get()) {
                for (Task task : taskDatas.values()) {
                    Object object = task.getTaskObject();
                    if (object instanceof ISpout) {
                        ((ISpout) object).close();
                    } else if (object instanceof IBolt) {
                        ((IBolt) object).cleanup();
                    } else {
                        LOG.error("unknown component object");
                    }
                }
            }
            LOG.info("Shut down executor " + executorData.getComponentId() + ":" + executorData.getExecutorId());
        } catch (Exception e) {
            throw Utils.wrapInRuntime(e);
        }
    }
}
