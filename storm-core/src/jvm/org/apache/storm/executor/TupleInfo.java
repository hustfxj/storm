package org.apache.storm.executor;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;

/**
 * @author JohnFang (xiaojian.fxj@alibaba-inc.com).
 */
public class TupleInfo implements Serializable {

    private static final long serialVersionUID = -3348670497595864118L;

    private int taskId;
    private Object messageId;
    private String stream;
    private List<Object> values;
    private long timestamp;
    private String id;

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object messageId) {
        this.messageId = messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }
}
