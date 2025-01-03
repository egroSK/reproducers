package com.reproducer;

import com.azure.cosmos.*;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Setup ChangeFeedProcessor and start consumption.
 */
public class EventProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

    public static void main(String[] args) {
        log.info("Starting EventProcessor");

        // Initialize CosmosAsyncClient, CosmosAsyncDatabase and CosmosAsyncContainers
        CosmosAsyncClient cosmosAsyncClient = CosmosConfig.cosmosAsyncClient();
        CosmosAsyncDatabase cosmosDatabase = CosmosConfig.cosmosDatabase(cosmosAsyncClient);
        CosmosAsyncContainer eventsCosmosContainer = CosmosConfig.eventsCosmosContainer(cosmosDatabase);
        CosmosAsyncContainer leasesCosmosContainer = CosmosConfig.leasesCosmosContainer(cosmosDatabase);

        // Initialize ChangeFeedProcessor
        ChangeFeedProcessor changeFeedProcessor = changeFeedProcessor(eventsCosmosContainer, leasesCosmosContainer);

        // Add shutdown hook to close ChangeFeed and CosmosAsyncClient when application is stopped
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            stopChangeFeedProcessor(changeFeedProcessor);
            CosmosConfig.closeCosmosAsyncClient(cosmosAsyncClient);
        }));

        // Start ChangeFeedProcessor & consume events
        try {
            startChangeFeedProcessor(changeFeedProcessor);
            // Keep thread running and consume events in the background until application is stopped
            while (true) {
                Thread.sleep(500);
            }
        } catch (Exception e) {
            log.error("EventProcessor failed: {}", e.getMessage(), e);
        }

        log.info("End EventProcessor");
    }

    private static ChangeFeedProcessor changeFeedProcessor(CosmosAsyncContainer eventsCosmosContainer, CosmosAsyncContainer leasesCosmosContainer) {
        log.info("Initializing ChangeFeedProcessor");
        ChangeFeedProcessorOptions options = new ChangeFeedProcessorOptions()
                .setFeedPollDelay(Duration.ofSeconds(2))
                .setMaxItemCount(64)
                .setLeasePrefix("reproducer")
                .setStartFromBeginning(true)
                // Just for debugging purpose, to not have a lease renew process running in the background while debugging
                .setLeaseRenewInterval(Duration.ofMinutes(10))
                .setLeaseExpirationInterval(Duration.ofMinutes(11));

        return new ChangeFeedProcessorBuilder()
                .hostName("local")
                .feedContainer(eventsCosmosContainer)
                .leaseContainer(leasesCosmosContainer)
                .options(options)
                .handleChanges(jsonNodes -> log.info("RECEIVED {} RECORDS", jsonNodes.size()))
                // .handleLatestVersionChanges(changeFeedProcessorItems -> log.info("RECEIVED {} RECORDS", changeFeedProcessorItems.size()))
                .buildChangeFeedProcessor();
    }

    private static void startChangeFeedProcessor(ChangeFeedProcessor changeFeedProcessor) {
        changeFeedProcessor.start()
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSubscribe(sub -> log.info("Starting ChangeFeedProcessor"))
                .doOnError(ex -> log.error("ChangeFeedProcessor start failed: {}", ex.getMessage(), ex))
                .doOnSuccess(v -> log.info("ChangeFeedProcessor started"))
                .block(Duration.ofSeconds(30));
    }

    public static void stopChangeFeedProcessor(ChangeFeedProcessor changeFeedProcessor) {
        changeFeedProcessor.stop()
                .thenReturn("done")
                .delayElement(Duration.ofMillis(500))
                .then()
                .doOnSubscribe(unused -> log.info("Stopping ChangeFeedProcessor"))
                .doOnError(ex -> log.error("ChangeFeedProcessor stop failed: {}", ex.getMessage(), ex))
                .doOnSuccess(unused -> log.info("ChangeFeedProcessor stopped successfully"))
                .block(Duration.ofSeconds(30));
    }

}
