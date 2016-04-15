package org.apache.storm.executor.error;

import org.apache.storm.Config;
import org.apache.storm.executor.ExecutorData;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ReportError implements IReportError {

    private static final Logger LOG = LoggerFactory.getLogger(ReportError.class);

    private final Map stormConf;
    private final ExecutorData executorData;
    private int maxPerInterval;
    private int errorIntervalSecs;
    private AtomicInteger intervalStartTime;
    private AtomicInteger intervalErrors;

    public ReportError(ExecutorData executorData) {
        this.executorData = executorData;
        this.stormConf = executorData.getStormConf();
        this.errorIntervalSecs = Utils.getInt(stormConf.get(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS));
        this.maxPerInterval = Utils.getInt(stormConf.get(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL));
        this.intervalStartTime = new AtomicInteger(Time.currentTimeSecs());
        this.intervalErrors = new AtomicInteger(0);
    }

    @Override
    public void report(Throwable error) {
        LOG.error("{}", error);
        if (Time.deltaSecs(intervalStartTime.get()) > errorIntervalSecs) {
            intervalErrors.set(0);
            intervalStartTime.set(Time.currentTimeSecs());
        }
        intervalErrors.incrementAndGet();
        if (intervalErrors.incrementAndGet() <= maxPerInterval) {
            try {
                executorData.getStormClusterState().reportError(executorData.getStormId(), executorData.getComponentId(), Utils.hostname(stormConf),
                        executorData.getWorkerTopologyContext().getThisWorkerPort().longValue(), error);
            } catch (UnknownHostException e) {
                throw Utils.wrapInRuntime(e);
            }

        }
    }
}
