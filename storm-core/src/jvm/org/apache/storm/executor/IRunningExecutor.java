package org.apache.storm.executor;

import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ExecutorStats;

import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public interface IRunningExecutor {

    ExecutorStats renderStats();
    List<Long> getExecutorId();
    void credenetialsChanged(Credentials credentials);
    boolean getBackPressureFlag();
}
