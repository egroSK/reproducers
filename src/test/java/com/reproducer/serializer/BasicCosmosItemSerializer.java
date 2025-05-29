package com.reproducer.serializer;

import com.azure.cosmos.CosmosItemSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;

import java.util.Map;

public class BasicCosmosItemSerializer extends CosmosItemSerializer {

    private final ObjectMapper objectMapper;
    private final MapType mapType;

    public BasicCosmosItemSerializer() {
        this.objectMapper = new ObjectMapper();
        ObjectMapperCustomizer.customizeObjectMapper(this.objectMapper);

        this.mapType = this.objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);
    }

    @Override
    public <T> Map<String, Object> serialize(T item) {
        return objectMapper.convertValue(item, mapType);
    }

    @Override
    public <T> T deserialize(Map<String, Object> jsonNodeMap, Class<T> classType) {
        return objectMapper.convertValue(jsonNodeMap, classType);
    }

}
