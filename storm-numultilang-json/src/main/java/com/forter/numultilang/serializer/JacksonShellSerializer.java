package com.forter.numultilang.serializer;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.forter.numultilang.ShellSerializer;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Serilizes/Deserializes shell messages using jackson.
 */
public class JacksonShellSerializer implements ShellSerializer {

    private static final int INITIAL_BUFFER_SIZE = 1024 * 1024; // 1 MB
    private final ObjectWriter objectWriter;
    private final ObjectReader objectReader;
    private final byte[] delimiter;
    private final String DELIMITER = "end";
    private final String NEW_LINE = "\n";
    private final byte NEW_LINE_BYTE = (byte)10;
    private final byte[] DELIMITER_BYTES = DELIMITER.getBytes(StandardCharsets.UTF_8);
    public final static ObjectMapper mapper = newObjectMapper();

    // a message buffer that does not include NEW_LINE
    ByteBuffer tempBuffer;
    byte[] tempBufferArray;

    private static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.addMixInAnnotations(ShellResponseMsg.class, ShellResponseMsgMixin.class);
        mapper.addMixInAnnotations(TopologyContext.class, TopologyContextMixin.class);
        return mapper;
    }

    public JacksonShellSerializer() {
        this.objectWriter = JacksonShellSerializer.mapper.writer();
        this.objectReader = JacksonShellSerializer.mapper.reader().withType(ShellResponseMsg.class);
        this.delimiter = (NEW_LINE + DELIMITER + NEW_LINE).getBytes(StandardCharsets.UTF_8);
        allocateTempBuffer(INITIAL_BUFFER_SIZE);
    }


    private boolean isDelimiter(byte[] array, int offset, int len) {
        if (len != DELIMITER_BYTES.length) {
            return false;
        }
        for (int pos = 0; pos < DELIMITER_BYTES.length ; pos++) {
            if (DELIMITER_BYTES[pos] != array[offset + pos]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterable<ByteBuffer> serialize(Object message) {
        List<ByteBuffer> chunks = Lists.newArrayListWithCapacity(2);
        try {
            //defaults to UTF-8
            chunks.add(ByteBuffer.wrap(objectWriter.writeValueAsBytes(message)));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
        chunks.add(ByteBuffer.wrap(delimiter));
        return chunks;
    }


    @Override
    public Iterable<ShellResponseMsg> deserialize(ByteBuffer chunk) {

        final List<ShellResponseMsg> messages = Lists.newLinkedList();

        int start = 0;
        int end = tempBuffer.position();

        // dynamic grow tempBuffer as needed
        if (tempBuffer.limit() < chunk.remaining()) {
            final ByteBuffer oldTempBuffer = tempBuffer;
            allocateTempBuffer(tempBuffer.limit() + chunk.remaining()*2)
                    .put(oldTempBuffer.array(),oldTempBuffer.arrayOffset()+start,end-start);
        }

        // copy new buffer
        tempBuffer.put(chunk);

        //look for new lines
        for (; end < tempBuffer.position() ; end++) {
            if (tempBufferArray[tempBuffer.arrayOffset()+end] == NEW_LINE_BYTE) {
                final int len = end-start;
                if (len > 0 &&
                        !isDelimiter(tempBufferArray, tempBuffer.arrayOffset() + start, len)) {
                    try {
                        final ShellResponseMsg msg =
                                objectReader.readValue(tempBufferArray, tempBuffer.arrayOffset() + start, len);
                        if (msg.getStream() == null)
                            msg.setStream(Utils.DEFAULT_STREAM_ID);
                        messages.add(msg);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
                start = end + 1;
            }
        }

        if (start > 0) {
            Preconditions.checkState(start <= tempBuffer.limit());
            //String s = chunkToString(tempBuffer);
            if (start == tempBuffer.position()) {
                //parsed all tempBuffer
                tempBuffer.clear();
            }
            else if (start < tempBuffer.limit()) {
                // copy remainder to new tempbuffer
                final ByteBuffer oldTempBuffer = tempBuffer;
                final int len = end-start;
                allocateTempBuffer(tempBuffer.limit())
                        .put(oldTempBuffer.array(), oldTempBuffer.arrayOffset() + start, len);
            }
        }

        return messages;
    }

    private static String chunkToString(ByteBuffer chunk) {
        return new String(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset()+ chunk.position(), StandardCharsets.UTF_8);
    }

    protected ByteBuffer allocateTempBuffer(int size) {
        tempBuffer = ByteBuffer.allocate(size);
        tempBufferArray = tempBuffer.array();
        return tempBuffer;
    }
}
