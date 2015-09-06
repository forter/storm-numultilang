package com.forter.numultilang.serializer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ShellResponseMsgMixin {

    @JsonProperty("need_task_ids")
    public void setNeedTaskIds(boolean needTaskIds) {}

    @JsonProperty("name")
    public void setMetricName(String metricName) {}

    @JsonProperty("params")
    public void setMetricParams(String metricParams) {}

    @JsonProperty("level")
    public void setLogLevel(int level) {}


}

