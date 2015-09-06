package com.forter.numultilang.messages;

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Connection message with shell bolt (the first message sent).
 */
public class ShellSetupMsg {
    private String pidDir;
    private Map conf;
    private TopologyContext context;

    public String getPidDir() {
        return pidDir;
    }

    public void setPidDir(String pidDir) {
        this.pidDir = pidDir;
    }

    public Map getConf() {
        return conf;
    }

    public void setConf(Map conf) {
        this.conf = conf;
    }

    public TopologyContext getContext() {
        return context;
    }

    public void setContext(TopologyContext context) {
        this.context = context;
    }
}
