package com.reproducer.serializer;

import com.azure.cosmos.CosmosItemSerializer;
import com.azure.cosmos.implementation.Utils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.lang.reflect.Method;
import java.util.List;

public class ObjectMapperCustomizer {

    public static void customizeObjectMapper(ObjectMapper objectMapper) {
        // Java 8 datetime types support
        objectMapper.registerModule(new JavaTimeModule());

        // Serialize datetime as ISO string
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Do not fail deserialization on unknown property
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static void customizeCosmosObjectMappers() {
        getCosmosObjectMappers().forEach(objectMapper -> {
            // Serialize datetime as ISO string
            objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        });
    }

    public static void removeCosmosObjectMappersCustomization() {
        getCosmosObjectMappers().forEach(objectMapper -> {
            // Disable serialization of datetime as ISO string (reset to default Cosmos SDK configuration)
            objectMapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        });
    }

    private static List<ObjectMapper> getCosmosObjectMappers() {
        return List.of(
                Utils.getSimpleObjectMapper(),
                getDefaultSerializerCosmosObjectMapper());
    }

    private static ObjectMapper getDefaultSerializerCosmosObjectMapper() {
        try {
            Method method = CosmosItemSerializer.class.getDeclaredMethod("getItemObjectMapper");
            method.setAccessible(true);
            return (ObjectMapper) method.invoke(CosmosItemSerializer.DEFAULT_SERIALIZER);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to invoke getItemObjectMapper method on CosmosItemSerializer.DEFAULT_SERIALIZER using reflection.", ex);
        }
    }

}
