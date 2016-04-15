package org.apache.storm.executor.error;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface IReportError {
    public void report(Throwable error);
}
