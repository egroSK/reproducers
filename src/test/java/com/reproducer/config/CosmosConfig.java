package com.reproducer.config;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.reproducer.serializer.BasicCosmosItemSerializer;
import com.reproducer.serializer.ObjectMapperCustomizer;
import com.reproducer.serializer.UnwrapCosmosItemSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Initialization of {@link CosmosAsyncClient}, used database ({@link CosmosAsyncDatabase}) and container ({@link CosmosAsyncContainer}).
 * <p>
 * It creates database and containers in DB if they not exist in Cosmos DB yet.
 */
public class CosmosConfig {

    private static final Logger log = LoggerFactory.getLogger(CosmosConfig.class);

    private static final String ENDPOINT = "";
    private static final String KEY = "";

    private static final String DATABASE_NAME = "custom-serializer-reproducer";
    private static final String CONTAINER_NAME = "data";

    public static CosmosAsyncClient cosmosAsyncClient(ConfigOption config) {
        log.debug("Initializing CosmosAsyncClient with config: {}", config);
        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint(CosmosConfig.ENDPOINT)
                .key(CosmosConfig.KEY)
                .contentResponseOnWriteEnabled(true);

        switch (config) {
            case NO_CUSTOMIZATION -> { /* Nothing to reconfigure, keep the default Cosmos configuration */ }
            case COSMOS_INTERNALS_RECONFIGURATION -> ObjectMapperCustomizer.customizeCosmosObjectMappers();
            case BASIC_CUSTOM_SERIALIZER -> cosmosClientBuilder.customItemSerializer(new BasicCosmosItemSerializer());
            case UNWRAP_CUSTOM_SERIALIZER -> cosmosClientBuilder.customItemSerializer(new UnwrapCosmosItemSerializer());
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

    public static void createDatabaseAndContainerIfNotExists() {
        log.trace("Creating database and container");
        CosmosAsyncClient cosmosAsyncClient = cosmosAsyncClient(ConfigOption.NO_CUSTOMIZATION);
        cosmosAsyncClient.createDatabaseIfNotExists(DATABASE_NAME).block(Duration.ofSeconds(30));
        cosmosAsyncClient.getDatabase(DATABASE_NAME).createContainerIfNotExists(CONTAINER_NAME, "/id").block(Duration.ofSeconds(30));
        closeCosmosAsyncClient(cosmosAsyncClient);
    }

    public static CosmosAsyncDatabase cosmosDatabase(CosmosAsyncClient client) {
        log.trace("Initializing CosmosAsyncDatabase: {}", DATABASE_NAME);
        return client.getDatabase(DATABASE_NAME);
    }

    public static CosmosAsyncContainer cosmosContainer(CosmosAsyncDatabase cosmosDatabase) {
        log.trace("Initializing CosmosAsyncContainer: {}", CONTAINER_NAME);
        return cosmosDatabase.getContainer(CONTAINER_NAME);
    }

    public static void closeCosmosAsyncClient(CosmosAsyncClient cosmosAsyncClient) {
        log.debug("Closing CosmosAsyncClient");
        cosmosAsyncClient.close();
        ObjectMapperCustomizer.removeCosmosObjectMappersCustomization();
    }

}
