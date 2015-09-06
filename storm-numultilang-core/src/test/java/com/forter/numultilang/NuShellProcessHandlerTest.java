package com.forter.numultilang;

import com.forter.numultilang.listeners.ShellProcessErrorListener;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.zaxxer.nuprocess.NuProcess;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Tests {@link NuShellProcessHandler}
 */
public class NuShellProcessHandlerTest implements ShellProcessErrorListener {

    NuShellProcessHandler h;
    @BeforeMethod
    public void beforeMethod() {
        h = new NuShellProcessHandler(new ToStringSerializer(), null, this, null);
    }

    @Test
    public void testStdinNoWrite() {
        ByteBuffer stdinStub = ByteBuffer.allocate(10);
        h.onStdinReady(stdinStub);
        assertThat(stdinStub.position()).isEqualTo(0);
    }

    @Test
    public void testStdinWriteShortMessage() {
        NuProcess nup = Mockito.mock(NuProcess.class);
        h.onPreStart(nup);
        h.writeMessage("Hello");
        ByteBuffer stdinStub = ByteBuffer.allocate(10);
        boolean cont = h.onStdinReady(stdinStub);
        assertThat(cont).isFalse();
        String actual = chunkToString(stdinStub);
        assertThat(actual).isEqualTo("Hello");
    }

    @Test
    public void testStdinWriteLongMessage() {
        NuProcess nup = Mockito.mock(NuProcess.class);
        h.onPreStart(nup);
        h.writeMessage("Hello World");

        ByteBuffer stdinStub1 = ByteBuffer.allocate(10);
        boolean cont1 = h.onStdinReady(stdinStub1);
        assertThat(cont1).isTrue();
        String actual1 = chunkToString(stdinStub1);
        assertThat(actual1).isEqualTo("Hello Worl");

        ByteBuffer stdinStub2 = ByteBuffer.allocate(10);
        boolean cont2 = h.onStdinReady(stdinStub2);
        assertThat(cont2).isFalse();
        String actual2 = chunkToString(stdinStub2);
        assertThat(actual2).isEqualTo("d");
    }

    @Override
    public void onShellProcessExit(int exitCode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onInternalError(Throwable t) {
        throw Throwables.propagate(t);
    }

    public static class ToStringSerializer implements ShellSerializer {

        @Override
        public Iterable<ByteBuffer> serialize(Object message) {
            ByteBuffer chunk = strToChunk(String.valueOf(message));
            return Lists.newArrayList(chunk);
        }

        @Override
        public Iterable<ShellResponseMsg> deserialize(ByteBuffer chunk) {
            throw new UnsupportedOperationException();
        }
    }

    private static ByteBuffer strToChunk(String str) {
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }

    private static String chunkToString(ByteBuffer chunk) {
        return new String(chunk.array(), chunk.arrayOffset() + chunk.position(), chunk.limit(), StandardCharsets.UTF_8);
    }
}
