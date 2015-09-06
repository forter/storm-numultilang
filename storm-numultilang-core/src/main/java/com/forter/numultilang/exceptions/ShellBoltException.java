package com.forter.numultilang.exceptions;

public abstract class ShellBoltException extends RuntimeException {
    public ShellBoltException(String message) {
        super(message);
    }

    public ShellBoltException(Throwable cause) {
        super(cause);
    }
}
