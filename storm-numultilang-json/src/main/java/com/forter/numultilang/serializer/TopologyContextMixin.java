package com.forter.numultilang.serializer;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = TopologyContextCustomSerializer.class)
public class TopologyContextMixin {

}
