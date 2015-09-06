package com.forter.numultilang.exceptions;

public class HeartbeatException extends ShellBoltException {
    public HeartbeatException() {
        super("subprocess heartbeat timeout");
    }
}
