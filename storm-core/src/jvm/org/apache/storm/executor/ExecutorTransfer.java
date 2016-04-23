package org.apache.storm.executor;

import clojure.lang.IFn;
import com.lmax.disruptor.EventHandler;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.MutableObject;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ExecutorTransfer implements EventHandler, Callable {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransfer.class);

    private final DisruptorQueue batchTransferQueue;
    private final KryoTupleSerializer serializer;
    private final String taskName;
    private final MutableObject cachedEmit;
    private final IFn transferFn;
    private final Map stormConf;

    public ExecutorTransfer(WorkerTopologyContext workerTopologyContext, DisruptorQueue batchTransferQueue, Map stormConf, IFn transferFn, String taskName) {
        this.batchTransferQueue = batchTransferQueue;
        this.serializer = new KryoTupleSerializer(stormConf, workerTopologyContext);
        this.taskName = taskName;
        this.cachedEmit = new MutableObject(new ArrayList<>());
        this.transferFn = transferFn;
        this.stormConf = stormConf;
    }

    public void transfer(int t, Tuple tuple) {
        AddressedTuple val = new AddressedTuple(t, tuple);
        if (Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false))
            LOG.info("TRANSFERING tuple {}", val);
        batchTransferQueue.publish(val);
    }

    @Override
    public Object call() throws Exception {
        batchTransferQueue.consumeBatchWhenAvailable(this);
        return 0;
    }

    public String getName() {
        return batchTransferQueue.getName();
    }

    @Override
    public void onEvent(Object o, long l, boolean b) throws Exception {
        ArrayList alist = (ArrayList) cachedEmit.getObject();
        alist.add(o);
        if (b) {
            transferFn.invoke(serializer, alist);
            cachedEmit.setObject(new ArrayList<>());
        }
    }
}
