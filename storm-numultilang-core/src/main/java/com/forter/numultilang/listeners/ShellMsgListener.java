package com.forter.numultilang.listeners;

import com.forter.numultilang.messages.ShellResponseMsg;

/**
 * Handles incoming bolt messages
 */
public interface ShellMsgListener {

    void onMessage(ShellResponseMsg msg);
}
