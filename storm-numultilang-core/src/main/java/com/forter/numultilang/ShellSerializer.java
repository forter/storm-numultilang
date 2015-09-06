package com.forter.numultilang;

import com.forter.numultilang.messages.ShellResponseMsg;

import java.nio.ByteBuffer;

public interface ShellSerializer {

    Iterable<ByteBuffer> serialize(Object message);

    Iterable<ShellResponseMsg> deserialize(ByteBuffer chunk);
}
