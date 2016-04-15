package org.apache.storm.executor.error;

import org.apache.storm.executor.ExecutorData;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class ReportErrorAndDie implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReportErrorAndDie.class);
    private final ExecutorData executorData;

    public ReportErrorAndDie(ExecutorData executorData) {
        this.executorData = executorData;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        executorData.getReportError().report(e);
        if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, e) || Utils.exceptionCauseIsInstanceOf(java.io.InterruptedIOException.class, e)) {
            LOG.info("Got interrupted excpetion shutting thread down...");
            executorData.getSuicideFn().run();
        }
    }
}
