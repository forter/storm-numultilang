package com.forter.numultilang.serializer;

import com.forter.numultilang.messages.ShellBoltMsg;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

/**
 * Tests {@link JacksonShellSerializer}
 */
public class JacksonShellSeializerTest {

    JacksonShellSerializer s;

    @BeforeMethod
    public void beforeMethod() {
        s = new JacksonShellSerializer();
    }

    @Test
    public void testSerialization() {
        ShellBoltMsg m = new ShellBoltMsg();
        m.setTask(5);
        List<ByteBuffer> chunks = Lists.newArrayList(s.serialize(m));
        assertThat(chunks.size()).isEqualTo(2);
        assertThat(chunkToString(chunks.get(0))).isEqualTo("{\"task\":5}");
        assertThat(chunkToString(chunks.get(1))).isEqualTo("\nend\n");
    }

    @Test
    public void testDeserialization() {
        List<ShellResponseMsg> msgs = Lists.newArrayList(s.deserialize(strToChunk("{\"task\":5}\nend\n")));
        assertThat(msgs.size()).isEqualTo(1);
        assertThat(msgs.get(0).getTask()).isEqualTo(5);
    }

    @Test
    public void testDeserializationTwoParts() {
        List<ShellResponseMsg> msgs1 = Lists.newArrayList(s.deserialize(strToChunk("{\"task\"")));
        assertThat(msgs1.size()).isEqualTo(0);
        List<ShellResponseMsg> msgs2 = Lists.newArrayList(s.deserialize(strToChunk(":5}\nend\n")));
        assertThat(msgs2.size()).isEqualTo(1);
        assertThat(msgs2.get(0).getTask()).isEqualTo(5);
    }

    @Test
    public void testDeserializationThreeParts() {
        List<ShellResponseMsg> msgs1 = Lists.newArrayList(s.deserialize(strToChunk("{\"task\"")));
        assertThat(msgs1.size()).isEqualTo(0);
        List<ShellResponseMsg> msgs2 = Lists.newArrayList(s.deserialize(strToChunk(":5}\ne")));
        assertThat(msgs2.size()).isEqualTo(1);
        assertThat(msgs2.get(0).getTask()).isEqualTo(5);
        List<ShellResponseMsg> msg3 = Lists.newArrayList(s.deserialize(strToChunk("nd\n")));
        assertThat(msg3.size()).isEqualTo(0);
    }

    @Test
    public void testDeserializationTwoLines() {
        List<ShellResponseMsg> msgs = Lists.newArrayList(s.deserialize(strToChunk("{\"task\":5}\nend\n{\"task\":6}\nend\n")));
        assertThat(msgs.size()).isEqualTo(2);
        assertThat(msgs.get(0).getTask()).isEqualTo(5);
        assertThat(msgs.get(1).getTask()).isEqualTo(6);
    }

    @Test
    public void testDeserializationLongMessage() {
        s.allocateTempBuffer(3);
        testDeserialization();
    }

    private static ByteBuffer strToChunk(String str) {
        return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
    }

    private static String chunkToString(ByteBuffer chunk) {
        return new String(chunk.array(), chunk.arrayOffset() + chunk.position(), chunk.limit(), StandardCharsets.UTF_8);
    }
}
