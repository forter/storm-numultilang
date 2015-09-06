package com.forter.numultilang;

import backtype.storm.Config;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.forter.numultilang.serializer.JacksonShellSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class PythonTest {

    private NuShellBolt bolt;
    private IOutputCollector collector;
    private TopologyContext context;
    private BlockingQueue<Object> emitted;
    private Path boltPath;
    private Map conf;

    public class StubCollector implements IOutputCollector {

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            emitted.add(tuple);
            return Lists.newArrayList(0);
        }

        @Override
        public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

        }

        @Override
        public void ack(Tuple input) {

        }

        @Override
        public void fail(Tuple input) {
        }

        @Override
        public void reportError(Throwable error) {
            emitted.add(error);
        }
    }

    @BeforeMethod
    public void beforeMethod() throws IOException {
        emitted = new LinkedBlockingQueue();
        boltPath = Paths.get("src/test/resources");
        if (!Files.exists(boltPath)) {
            boltPath = Paths.get("storm-numultilang-json", boltPath.toString()).toAbsolutePath();
            if (!Files.exists(boltPath)) {
                throw Throwables.propagate(new FileNotFoundException(boltPath.toString()));
            }
        }
        collector = new StubCollector();
        conf = Maps.newHashMap();
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_EMIT_TIMEOUT_MS, Integer.MAX_VALUE);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_SETUP_TIMEOUT_MS, Integer.MAX_VALUE);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_SERIALIZER, JacksonShellSerializer.class.getCanonicalName());
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_STDERR_MAX_LENGTH, 1024 * 1024);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_STDERR_MAX_LENGTH, 1024 * 1024);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_HEARTBEAT_PERIOD_MS, 10*1000);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_HEARTBEAT_TIMEOUT_MS, Integer.MAX_VALUE);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_CODE_DIR, boltPath.toString());
        context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-component-id");
        when(context.getPIDDir()).thenReturn(Files.createTempDirectory("test-pids").toAbsolutePath().toString());
    }

    @Test
    public void testSpiltSentenceBolt() throws Throwable {
        bolt = new NuShellBolt("python", boltPath.resolve("splitsentence.py").toString());
        bolt.prepare(conf, context, collector);
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("test-component-bolt");
        when(tuple.getSourceStreamId()).thenReturn("test-stream-id");
        when(tuple.getSourceTask()).thenReturn(0);
        when(tuple.getValues()).thenReturn(Lists.newArrayList((Object) "Hello World"));
        bolt.execute(tuple);
        assertEmittedString("Hello");
        assertEmittedString("World");
    }

    @Test
    public void testReadFileBolt() throws Throwable {
        bolt = new NuShellBolt("python", boltPath.resolve("readfile.py").toString());
        bolt.prepare(conf, context, collector);
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("test-component-bolt");
        when(tuple.getSourceStreamId()).thenReturn("test-stream-id");
        when(tuple.getSourceTask()).thenReturn(0);
        when(tuple.getValues()).thenReturn(Lists.newArrayList((Object) ""));
        bolt.execute(tuple);
        assertEmittedString("Hello World");
    }

    private void assertEmittedString(String value) throws Throwable {
        final Object emit1 = emitted.take();
        if (emit1 instanceof Throwable) {
            throw (Throwable)emit1;
        }
        assertThat(((List<Object>)emit1).get(0)).isEqualTo(value);
    }
}
