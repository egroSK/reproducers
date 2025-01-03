package com.reproducer;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Initialization of {@link CosmosAsyncClient}, used database ({@link CosmosAsyncDatabase}) and containers ({@link CosmosAsyncContainer}).
 * <p>
 * It creates database and containers in DB if they not exist in Cosmos DB yet.
 */
public class CosmosConfig {

    private static final Logger log = LoggerFactory.getLogger(CosmosConfig.class);

    private static final String ENDPOINT = "<<YOUR ENDPOINT>>";
    private static final String KEY = "<<YOUR KEY>>";

    private static final String DATABASE_NAME = "change-feed-reproducer";
    private static final String EVENTS_CONTAINER_NAME = "events";
    private static final String LEASES_CONTAINER_NAME = "leases";

    public static CosmosAsyncClient cosmosAsyncClient() {
        log.info("Initializing CosmosAsyncClient");
        return new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase cosmosDatabase(CosmosAsyncClient client) {
        log.info("Initializing CosmosAsyncDatabase: {}", DATABASE_NAME);
        client.createDatabaseIfNotExists(DATABASE_NAME).block(Duration.ofSeconds(30));
        return client.getDatabase(DATABASE_NAME);
    }

    public static CosmosAsyncContainer eventsCosmosContainer(CosmosAsyncDatabase cosmosDatabase) {
        log.info("Initializing CosmosAsyncContainer: {}", EVENTS_CONTAINER_NAME);
        cosmosDatabase.createContainerIfNotExists(EVENTS_CONTAINER_NAME, "/id").block(Duration.ofSeconds(30));
        return cosmosDatabase.getContainer(EVENTS_CONTAINER_NAME);
    }

    public static CosmosAsyncContainer leasesCosmosContainer(CosmosAsyncDatabase cosmosDatabase) {
        log.info("Initializing CosmosAsyncContainer: {}", LEASES_CONTAINER_NAME);
        cosmosDatabase.createContainerIfNotExists(LEASES_CONTAINER_NAME, "/id").block(Duration.ofSeconds(30));
        return cosmosDatabase.getContainer(LEASES_CONTAINER_NAME);
    }

    public static void closeCosmosAsyncClient(CosmosAsyncClient cosmosAsyncClient) {
        log.info("Closing CosmosAsyncClient");
        cosmosAsyncClient.close();
    }

}
