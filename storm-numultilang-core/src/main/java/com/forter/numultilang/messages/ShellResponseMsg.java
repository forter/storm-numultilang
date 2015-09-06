package com.forter.numultilang.messages;

import backtype.storm.multilang.ShellMsg;

public final class ShellResponseMsg extends ShellMsg {

    ShellResponseMsg() {
        this.setNeedTaskIds(true); //this is the default in the original storm json parser
    }

    private int pid;

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }
}
