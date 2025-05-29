package com.reproducer.serializer;

import com.azure.cosmos.CosmosItemSerializer;
import com.azure.cosmos.implementation.Constants;
import com.azure.cosmos.implementation.JsonSerializable;
import com.azure.cosmos.implementation.ObjectNodeMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;

import java.util.Map;

/**
 * Just for demonstration purposes, not for production use.
 */
public class UnwrapCosmosItemSerializer extends CosmosItemSerializer {

    private final ObjectMapper objectMapper;
    private final MapType mapType;

    public UnwrapCosmosItemSerializer() {
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
        // Document/JsonSerializable deserialization (Used like this in DefaultCosmosItemSerializer)
        if (JsonSerializable.class.isAssignableFrom(classType)) {
            return (T) JsonSerializable.instantiateFromObjectNodeAndType(((ObjectNodeMap) jsonNodeMap).getObjectNode(), classType);
        }

        // VALUE unwrapping (Used like this in JsonSerializable#toObjectFromObjectNode called from ValueUnwrapCosmosItemSerializer)
        if (jsonNodeMap.containsKey(Constants.Properties.VALUE) && jsonNodeMap.size() == 1) {
            return objectMapper.convertValue(jsonNodeMap.get(Constants.Properties.VALUE), classType);
        }

        // Rest of it using ObjectMapper
        return objectMapper.convertValue(jsonNodeMap, classType);
    }

}
