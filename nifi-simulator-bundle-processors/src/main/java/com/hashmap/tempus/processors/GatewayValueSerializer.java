package com.hashmap.tempus.processors;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class GatewayValueSerializer extends StdSerializer<GatewayValue> {

    public GatewayValueSerializer() {
        this(null);
    }

    public GatewayValueSerializer(Class<GatewayValue> t){
        super(t);
    }

    @Override
    public void serialize(GatewayValue gatewayValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeObjectField(gatewayValue.getDeviceName(), gatewayValue.getDataValues());

        jsonGenerator.writeEndObject();
    }
}
