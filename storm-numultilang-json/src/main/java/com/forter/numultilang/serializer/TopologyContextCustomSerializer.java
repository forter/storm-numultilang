package com.forter.numultilang.serializer;


import backtype.storm.task.TopologyContext;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class TopologyContextCustomSerializer extends JsonSerializer<TopologyContext> {

    /**
     * @see TopologyContext#toJSONString()
     */
    @Override
    public void serialize(TopologyContext value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        jgen.writeObjectField("task->component", value.getTaskToComponent());
        jgen.writeNumberField("taskid", value.getThisTaskId());
        jgen.writeEndObject();
        // TODO: jsonify StormTopology
        // at the minimum should send source info
    }
}