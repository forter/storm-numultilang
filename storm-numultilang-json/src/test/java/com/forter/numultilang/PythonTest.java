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

    @JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE)
    public interface TopologyContextMixin {
    }

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
        JacksonShellSerializer.mapper.addMixInAnnotations(TopologyContext.class, TopologyContextMixin.class);
        String path = "src/test/resources";
        Path boltFile = Paths.get(path, "splitsentence.py");
        if (!Files.exists(boltFile)) {
            boltFile = Paths.get("storm-numultilang-json", boltFile.toString());
            if (!Files.exists(boltFile)) {
                throw Throwables.propagate(new FileNotFoundException(path));
            }
        }
        bolt = new NuShellBolt("python", boltFile.toAbsolutePath().toString());
        collector = new StubCollector();
        Map conf = Maps.newHashMap();
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_EMIT_TIMEOUT_MS, Long.MAX_VALUE);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_SETUP_TIMEOUT_MS, Long.MAX_VALUE);
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_SERIALIZER, JacksonShellSerializer.class.getCanonicalName());
        conf.put(NuShellConfig.TOPOLOGY_NUMULTILANG_STDERR_MAX_LENGTH, 1024 * 1024);
        conf.put(Config.SUPERVISOR_WORKER_TIMEOUT_SECS, 1);
        context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-component-id");
        when(context.getPIDDir()).thenReturn(Files.createTempDirectory("test-pids").toAbsolutePath().toString());
        bolt.prepare(conf, context, collector);
    }

    @Test
    public void testPythonBolt() throws Throwable {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getSourceComponent()).thenReturn("test-component-bolt");
        when(tuple.getSourceStreamId()).thenReturn("test-stream-id");
        when(tuple.getSourceTask()).thenReturn(0);
        when(tuple.getValues()).thenReturn(Lists.newArrayList((Object) "Hello World"));
        bolt.execute(tuple);
        assertEmittedString("Hello");
        assertEmittedString("World");
    }

    private void assertEmittedString(String value) throws Throwable {
        final Object emit1 = emitted.take();
        if (emit1 instanceof Throwable) {
            throw (Throwable)emit1;
        }
        assertThat(((List<Object>)emit1).get(0)).isEqualTo(value);
    }
}
