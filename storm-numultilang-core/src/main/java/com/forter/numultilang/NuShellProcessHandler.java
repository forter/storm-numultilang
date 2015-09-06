package com.forter.numultilang;

import com.forter.numultilang.listeners.ShellMsgListener;
import com.forter.numultilang.listeners.ShellProcessErrorListener;
import com.forter.numultilang.listeners.ShellProcessStdErrListener;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessHandler;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @see NuProcessHandler
 */
public class NuShellProcessHandler extends NuAbstractProcessHandler implements NuProcessHandler {

    private final ConcurrentLinkedQueue<ByteBuffer> pendingWrites;
    private final ShellMsgListener messageListener;
    private final ShellProcessErrorListener errorListener;
    private final ShellProcessStdErrListener stdErrListener;
    private final ShellSerializer serializer;
    private NuProcess nuProcess;


    public NuShellProcessHandler(
            ShellSerializer serializer,
            ShellMsgListener messageListener,
            ShellProcessErrorListener errorListener,
            ShellProcessStdErrListener stdErrListener) {
        this.stdErrListener = stdErrListener;

        pendingWrites = new ConcurrentLinkedQueue();

        this.serializer = serializer;
        this.messageListener = messageListener;
        this.errorListener = errorListener;

    }

    public void writeMessage(Object message) {
        for (ByteBuffer bytes: serializer.serialize(message)) {
            pendingWrites.add(bytes);
        }
        nuProcess.wantWrite();
    }

    @Override
    public void onPreStart(NuProcess nuProcess) {
        this.nuProcess = nuProcess;
        super.onPreStart(nuProcess);
    }

    @Override
    public void onStart(NuProcess nuProcess) {
        super.onStart(nuProcess);
    }

    @Override
    public void onExit(int exitCode) {
        super.onExit(exitCode);
        errorListener.onShellProcessExit(exitCode);
    }

    @Override
    public void onStdout(ByteBuffer buffer, boolean closed) {
        try {
            for (ShellResponseMsg msg : serializer.deserialize(buffer)) {
                messageListener.onMessage(msg);
            }

            super.onStdout(buffer, closed);
        }
        catch (Throwable t) {
            errorListener.onInternalError(t);
        }
    }


    @Override
    public void onStderr(ByteBuffer buffer, boolean closed) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        stdErrListener.onShellProcessStdError(new String(bytes, StandardCharsets.UTF_8));
        super.onStderr(buffer, closed);
    }

    /** {@inheritDoc} */
    @Override
    public boolean onStdinReady(ByteBuffer directStdinBuffer) {
        try {
            return onStdinReadyInternal(directStdinBuffer);
        }
        catch (Throwable t) {
            errorListener.onInternalError(t);
            return false;
        }
    }

    private boolean onStdinReadyInternal(ByteBuffer directStdinBuffer) {
        boolean hasMore = false;
        while(!pendingWrites.isEmpty()) {
            final int stdinLimit = directStdinBuffer.limit();
            // copy the next buffer into our direct buffer (inBuffer)
            ByteBuffer nextBuffer = pendingWrites.peek();
            final int nextBufferRemaining = nextBuffer.remaining();

            if (nextBufferRemaining > stdinLimit) {
                ByteBuffer slice = nextBuffer.slice();
                slice.limit(stdinLimit);
                directStdinBuffer.put(slice);
                nextBuffer.position(nextBuffer.position() + stdinLimit);
                hasMore = true;
                break;
            } else {
                directStdinBuffer.put(nextBuffer);
                pendingWrites.poll();
            }
        }
        directStdinBuffer.flip();
        return hasMore;
    }
}

