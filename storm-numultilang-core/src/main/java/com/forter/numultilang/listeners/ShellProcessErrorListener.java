package com.forter.numultilang.listeners;

import com.forter.numultilang.NuShellProcessHandler;

/**
 * Handles exceptions occured in {@link NuShellProcessHandler}
 */
public interface ShellProcessErrorListener {

    void onShellProcessExit(int exitCode);

    void onInternalError(Throwable t);
}
