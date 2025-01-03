package com.reproducer;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosItemResponse;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Helper class to create events in Cosmos DB. Change feed will consume these events.
 */
class EventCreator {

    private static final Logger log = LoggerFactory.getLogger(EventCreator.class);

    public static void main(String[] args) {
        // Initialize CosmosAsyncClient, CosmosAsyncDatabase and CosmosAsyncContainers
        CosmosAsyncClient cosmosAsyncClient = CosmosConfig.cosmosAsyncClient();
        CosmosAsyncDatabase cosmosDatabase = CosmosConfig.cosmosDatabase(cosmosAsyncClient);
        CosmosAsyncContainer eventsCosmosContainer = CosmosConfig.eventsCosmosContainer(cosmosDatabase);

        // Create events in DB
        String prefix = "COSMO-001-";
        for (int i = 1; i <= 300; i++) {
            Event event = new Event(prefix + StringUtils.leftPad(String.valueOf(i), 4, "0"));
            CosmosItemResponse<Event> response = eventsCosmosContainer.createItem(event).block(Duration.ofSeconds(5));
            log.info("Created event {} => statusCode: {}", event.getId(), response.getStatusCode());
        }
    }

    public static class Event {

        private String id;

        public Event(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

}