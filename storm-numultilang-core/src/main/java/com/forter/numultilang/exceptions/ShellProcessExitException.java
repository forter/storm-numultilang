package com.forter.numultilang.exceptions;

public class ShellProcessExitException extends ShellBoltException {
    public ShellProcessExitException(int exitCode) {
        super("Child process exited with exit code " + exitCode);
    }
}
