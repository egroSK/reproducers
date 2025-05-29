package com.reproducer.config;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.implementation.JsonSerializable;
import com.reproducer.serializer.BasicCosmosItemSerializer;
import com.reproducer.serializer.UnwrapCosmosItemSerializer;

/**
 * Different {@link CosmosAsyncClient} configurations for which tests in {@link com.reproducer.ReproducerTest} can be executed to compare behavior.
 */
public enum ConfigOption {

    /**
     * {@link CosmosAsyncClient} without registration of {@code customItemSerializer} or reconfiguration of the internal ObjectMapper.
     */
    NO_CUSTOMIZATION,

    /**
     * No {@code customItemSerializer} registered. Internal Cosmos SDK ObjectMapper is reconfigured.
     */
    COSMOS_INTERNALS_RECONFIGURATION,

    /**
     * {@link CosmosAsyncClient} with registration of {@code customItemSerializer}. It uses ObjectMapper with the required configuration for serialization and deserialization of
     * data stored in Cosmos. However, it does not work as expected due to some issues.
     *
     * @see BasicCosmosItemSerializer
     */
    BASIC_CUSTOM_SERIALIZER,

    /**
     * {@link CosmosAsyncClient} with registration of {@code customItemSerializer}. It uses an ObjectMapper configured for serialization and deserialization of data stored in
     * Cosmos. Additionally, it implements a hack for deserializing internal {@link JsonSerializable} objects and unwrapping the {@code VALUE} field as a demonstration that such a
     * hack could work. However, this approach is very fragile and relies on internal Cosmos classes, so it is definitely not the way to use it.
     *
     * @see UnwrapCosmosItemSerializer
     */
    UNWRAP_CUSTOM_SERIALIZER,

}
