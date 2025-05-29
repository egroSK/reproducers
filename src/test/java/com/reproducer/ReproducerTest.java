package com.reproducer;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.reproducer.config.ConfigOption;
import com.reproducer.config.CosmosConfig;
import com.reproducer.model.Transaction;
import com.reproducer.model.TransactionsGroupBy;
import com.reproducer.model.TransactionsSum;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproducer for issues with Cosmos DB serialization and deserialization.
 * <p>
 * Before executing tests, configure connection string in {@link CosmosConfig} class ({@code ENDPOINT} and {@code KEY} fields).
 */
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class ReproducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReproducerTest.class);
    private static final String CONFIG_OPTIONS_SOURCE_NAME = "com.reproducer.ReproducerTest#CONFIG_OPTIONS";

    /**
     * List of {@link ConfigOption} for which {@code @ParametrizedTest} are executed. Options can be commented out in this list to focus on testing/debugging only of the selected
     * config.
     */
    private static final List<ConfigOption> CONFIG_OPTIONS = List.of(
            ConfigOption.NO_CUSTOMIZATION,
            ConfigOption.COSMOS_INTERNALS_RECONFIGURATION,
            ConfigOption.BASIC_CUSTOM_SERIALIZER,
            ConfigOption.UNWRAP_CUSTOM_SERIALIZER
    );

    private static final String TRANSACTION_ID = "TRX-001";
    private static final LocalDate CREATION_DATE = LocalDate.of(2025, 5, 27);

    @BeforeAll
    static void beforeAll() {
        CosmosConfig.createDatabaseAndContainerIfNotExists();
    }

    /**
     * Tests checking whether data is serialized as expected, indicating that the serializer/ObjectMapper is configured correctly.
     */
    @Nested
    @Order(1)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class DateTimeSerialization {

        /**
         * Insert LocalDate to DB and check how it was serialized. The requirement is to serialize it as an ISO string.
         * <ul>
         *     <li>NO_CUSTOMIZATION - ERROR (Unexpected type - ArrayList instead of String) - Cosmos serializes LocalDate as an array by default, because ObjectMapper feature
         *     WRITE_DATES_AS_TIMESTAMPS is enabled.</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK - internal ObjectMapper is reconfigured</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - OK - uses custom serializer with reconfigured ObjectMapper</li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - uses custom serializer with reconfigured ObjectMapper</li>
         * </ul>
         */
        @Order(1)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("LocalDate field is written to DB as ISO string")
        void localDateIsSerializedAsIsoString(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // WHEN
                Map<String, Object> resultTransaction = readTransactionAsMap(cosmosAsyncContainer);

                // THEN
                LOG.info("Result transaction: {}", resultTransaction);
                assertNotNull(resultTransaction);

                Object creationDate = resultTransaction.get("creationDate");
                LOG.info("Result value: {}, type: {}", creationDate, creationDate.getClass().getName());

                String creationDateAsString = assertInstanceOf(String.class, creationDate, "LocalDate is not serialized as String in JSON saved in DB");
                assertEquals("2025-05-27", creationDateAsString);
            });
        }

        /**
         * Insert OffsetDateTime to DB and check how it was serialized. The requirement is to serialize it as an ISO string.
         * <ul>
         *     <li>NO_CUSTOMIZATION - ERROR (Unexpected type - Integer instead of String) - Cosmos serializes OffsetDateTime as a number by default, because ObjectMapper feature
         *     WRITE_DATES_AS_TIMESTAMPS is enabled.</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK - internal ObjectMapper is reconfigured</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - OK - uses custom serializer with reconfigured ObjectMapper</li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - uses custom serializer with reconfigured ObjectMapper</li>
         * </ul>
         */
        @Order(2)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("OffsetDateTime field is written to DB as ISO string")
        void offsetDateTimeIsSerializedAsIsoString(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // WHEN
                Map<String, Object> resultTransaction = readTransactionAsMap(cosmosAsyncContainer);

                // THEN
                LOG.info("Result transaction: {}", resultTransaction);
                assertNotNull(resultTransaction);

                Object creationDateTime = resultTransaction.get("creationDateTime");
                LOG.info("Result value: {}, type: {}", creationDateTime, creationDateTime.getClass().getName());

                String creationDateTimeAsString = assertInstanceOf(String.class, creationDateTime, "OffsetDateTime is not serialized as String in JSON saved in DB");
                assertEquals("2025-05-27T15:23:44+04:00", creationDateTimeAsString);
            });
        }

        /**
         * LocalDate in SqlParameter should be serialized as an ISO string. Otherwise, it will not find data with LocalDate serialized as ISO string in DB. Note: SqlParameter uses
         * {@link com.azure.cosmos.implementation.Utils#getSimpleObjectMapper()} for serialization.
         * <ul>
         *     <li>NO_CUSTOMIZATION - ERROR (Unexpected type - ArrayNode instead of String) - Cosmos serializes LocalDate in SqlParameter as an array by default, because
         *     ObjectMapper used by SqlParameter has WRITE_DATES_AS_TIMESTAMPS feature enabled.</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK - internal ObjectMapper used by SqlParameter is reconfigured</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR (Unexpected type - ArrayNode instead of String) - SqlParameter does not use custom serializer with reconfigured ObjectMapper</li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - ERROR (Unexpected type - ArrayNode instead of String) - SqlParameter does not use custom serializer with reconfigured
         *     ObjectMapper</li>
         * </ul>
         */
        @Order(3)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("LocalDate field is serialized in SqlParameter in ISO string format")
        void localDateInSqlParameterIsSerializedAsIsoString(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // WHEN
                SqlParameter sqlParameter = new SqlParameter("creationDate", CREATION_DATE);

                // THEN
                Object creationDate = sqlParameter.getValue(Object.class);
                LOG.info("Result value: {}, type: {}", creationDate, creationDate.getClass().getName());

                String creationDateAsString = assertInstanceOf(String.class, creationDate, "LocalDate is not serialized as String in SqlParameter");
                assertEquals("2025-05-27", creationDateAsString);
            });
        }

        /**
         * Query by LocalDate field. DB record is found when LocalDate serialized in DB and LocalDate serialized by SqlParameter has the same value.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK - with default configuration, LocalDate is serialized in DB as an array and in SqlParameter as an array</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK - internal ObjectMapper is reconfigured to serialize datetime as ISO string. Because of this LocalDate is serialized in
         *     DB as in ISO string and in SqlParameter as an ISO string.</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR (nothing found) - Nothing is found, because LocalDate in DB is serialized as ISO string, but SqlParameter serializes LocalDate
         *     as an array, because it does not use custom serializer with reconfigured ObjectMapper.</li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - ERROR (nothing found) - Nothing is found, because LocalDate in DB is serialized as ISO string, but SqlParameter serializes
         *     LocalDate as an array, because it does not use custom serializer with reconfigured ObjectMapper.</li>
         * </ul>
         */
        @Order(4)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Search by LocalDate is working")
        void searchByLocalDate(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT * FROM c WHERE c.creationDate = @creationDate";
                List<SqlParameter> params = List.of(new SqlParameter("@creationDate", CREATION_DATE));

                // WHEN
                Transaction resultTransaction = queryItem(cosmosAsyncContainer, query, params, Transaction.class);

                // THEN
                LOG.info("Result transaction: {}", resultTransaction);
                assertNotNull(resultTransaction);
                assertEquals(TRANSACTION_ID, resultTransaction.getId());
                assertEquals(CREATION_DATE, resultTransaction.getCreationDate());
            });
        }

    }

    /**
     * Tests with SELECTS which fail when {@code customItemSerializer} ({@link ConfigOption#BASIC_CUSTOM_SERIALIZER}) is used.
     */
    @Order(2)
    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class Query {

        /**
         * Query with VALUE. The result is internally deserialized in two steps. In the first step it is deserialized to Document. Then it retrieves VALUE from the Document and
         * converts it to the requested type.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR<br/><pre>
         *         com.fasterxml.jackson.databind.exc.MismatchedInputException: Cannot deserialize value of type `java.lang.String` from Object value (token `JsonToken
         *         .START_OBJECT`)
         *           at [Source: UNKNOWN; byte offset: #UNKNOWN]
         *          	at com.fasterxml.jackson.databind.exc.MismatchedInputException.from(MismatchedInputException.java:59)
         *          	at com.fasterxml.jackson.databind.DeserializationContext.reportInputMismatch(DeserializationContext.java:1767)
         *          	at com.fasterxml.jackson.databind.DeserializationContext.handleUnexpectedToken(DeserializationContext.java:1541)</pre>
         *         </li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - simulates two steps deserialization as internally done by Cosmos</li>
         * </ul>
         */
        @Order(1)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Query with VALUE is working")
        void queryWithValue(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT DISTINCT VALUE c.id FROM c";

                // WHEN
                String resultTransactionId = queryItem(cosmosAsyncContainer, query, String.class);

                // THEN
                LOG.info("Result transactionId: {}", resultTransactionId);
                assertEquals(TRANSACTION_ID, resultTransactionId);
            });
        }

        /**
         * Query with SUM (aggregation). The result is internally deserialized in two steps. In the first step it is deserialized to Document. Then it retrieves {@code payload}
         * from the Document and converts it to the requested type.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR<br/><pre>
         *         java.lang.IllegalStateException: Underlying object does not have an 'payload' field.
         *          	at com.azure.cosmos.implementation.query.AggregateDocumentQueryExecutionContext$RewrittenAggregateProjections.<init>(AggregateDocumentQueryExecutionContext.java:151)
         *          	at com.azure.cosmos.implementation.query.AggregateDocumentQueryExecutionContext.lambda$drainAsync$0(AggregateDocumentQueryExecutionContext.java:88)</pre>
         *         </li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - simulates two steps deserialization as internally done by Cosmos</li>
         * </ul>
         */
        @Order(2)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Query with SUM is working")
        void queryWithSum(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT SUM(c.amount) AS transactionsAmount FROM c";

                // WHEN
                TransactionsSum transactionsSum = queryItem(cosmosAsyncContainer, query, TransactionsSum.class);

                // THEN
                LOG.info("Result transactionsSum: {}", transactionsSum);
                assertNotNull(transactionsSum);
                assertEquals(0, BigDecimal.TEN.compareTo(transactionsSum.getTransactionsAmount()));
            });
        }

        /**
         * Query with ORDER BY. The result is internally deserialized in two steps. In the first step it is deserialized to Document. Then it retrieves {@code payload} from the
         * Document and converts it to the requested type.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR<br/><pre>
         *         java.lang.NullPointerException: Cannot invoke "Object.getClass()" because "object" is null
         *          	at com.azure.cosmos.implementation.query.orderbyquery.OrderByRowResult.getPayload(OrderByRowResult.java:43)
         *          	at com.azure.cosmos.implementation.query.OrderByDocumentQueryExecutionContext$ItemToPageTransformer.lambda$apply$5(OrderByDocumentQueryExecutionContext.java:695)</pre>
         *         </li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - simulates two steps deserialization as internally done by Cosmos</li>
         * </ul>
         */
        @Order(3)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Query with ORDER BY is working")
        void queryWithOrderBy(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT * FROM c ORDER BY c.id";

                // WHEN
                Transaction transaction = queryItem(cosmosAsyncContainer, query, Transaction.class);

                // THEN
                LOG.info("Result transaction: {}", transaction);
                assertNotNull(transaction);
                assertEquals(TRANSACTION_ID, transaction.getId());
            });
        }

        /**
         * Query with GROUP BY. The result is internally deserialized in two steps. In the first step it is deserialized to Document. Then it uses {@code groupByItems} from the
         * Document.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR<br/><pre>
         *         java.lang.IllegalStateException: Underlying object does not have an 'groupByItems' field.
         *          	at com.azure.cosmos.implementation.query.GroupByDocumentQueryExecutionContext$RewrittenGroupByProjection.getGroupByItems
         *          	(GroupByDocumentQueryExecutionContext.java:184)
         *          	at com.azure.cosmos.implementation.query.GroupingTable.addPayLoad(GroupingTable.java:38)
         *          	at com.azure.cosmos.implementation.query.GroupByDocumentQueryExecutionContext.aggregateGroupings(GroupByDocumentQueryExecutionContext.java:152)
         *          	at com.azure.cosmos.implementation.query.GroupByDocumentQueryExecutionContext.lambda$drainAsync$1(GroupByDocumentQueryExecutionContext.java:97)
         *         </li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - simulates two steps deserialization as internally done by Cosmos</li>
         * </ul>
         */
        @Order(4)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Query with GROUP BY is working")
        void queryWithGroupBy(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT c.amount, COUNT(c.amount) as count FROM c GROUP BY c.amount";

                // WHEN
                TransactionsGroupBy transactionsGroupBy = queryItem(cosmosAsyncContainer, query, TransactionsGroupBy.class);

                // THEN
                LOG.info("Result transactionsGroupBy: {}", transactionsGroupBy);
                assertNotNull(transactionsGroupBy);
                assertEquals(0, BigDecimal.TEN.compareTo(transactionsGroupBy.getAmount()));
                assertEquals(1, transactionsGroupBy.getCount());
            });
        }

        /**
         * Query with VALUE COUNT. The result is internally deserialized in two steps. In the first step it is deserialized to Document. Then it uses {@code item} from the
         * Document.
         * <ul>
         *     <li>NO_CUSTOMIZATION - OK</li>
         *     <li>COSMOS_INTERNALS_RECONFIGURATION - OK</li>
         *     <li>BASIC_CUSTOM_SERIALIZER - ERROR<br/><pre>
         *         java.lang.NullPointerException: Cannot invoke "Object.toString()" because "item" is null
         *          	at com.azure.cosmos.implementation.query.aggregation.CountAggregator.aggregate(CountAggregator.java:11)
         *          	at com.azure.cosmos.implementation.query.SingleGroupAggregator$AggregateAggregateValue.addValue(SingleGroupAggregator.java:253)
         *          	at com.azure.cosmos.implementation.query.SingleGroupAggregator$SelectValueAggregateValues.addValues(SingleGroupAggregator.java:96)
         *          	at com.azure.cosmos.implementation.query.AggregateDocumentQueryExecutionContext.lambda$drainAsync$0(AggregateDocumentQueryExecutionContext.java:91)
         *         </li>
         *     <li>UNWRAP_CUSTOM_SERIALIZER - OK - simulates two steps deserialization as internally done by Cosmos</li>
         * </ul>
         */
        @Order(5)
        @ParameterizedTest
        @FieldSource(CONFIG_OPTIONS_SOURCE_NAME)
        @DisplayName("Query with VALUE COUNT is working")
        void queryWithValueCount(ConfigOption configOption) {
            executeTest(configOption, cosmosAsyncContainer -> {
                // GIVEN
                String query = "SELECT VALUE COUNT(c) FROM c";

                // WHEN
                Integer resultCount = queryItem(cosmosAsyncContainer, query, Integer.class);

                // THEN
                LOG.info("Result count: {}", resultCount);
                assertNotNull(resultCount);
                assertEquals(1, resultCount);
            });
        }

    }

    /**
     * Helper method for executing tests. It inits {@link CosmosAsyncClient}, creates testing transaction in database and executes the test function. After test function is
     * finished, it deletes testing transaction from database and closes {@link CosmosAsyncClient}.
     * <p>
     * It inits {@link CosmosAsyncClient} for each test from scratch to be sure it has the correct configuration.
     */
    static void executeTest(ConfigOption configOption, Consumer<CosmosAsyncContainer> testFunction) {
        CosmosAsyncClient cosmosAsyncClient = CosmosConfig.cosmosAsyncClient(configOption);
        CosmosAsyncDatabase cosmosAsyncDatabase = CosmosConfig.cosmosDatabase(cosmosAsyncClient);
        CosmosAsyncContainer cosmosAsyncContainer = CosmosConfig.cosmosContainer(cosmosAsyncDatabase);
        try {
            createTransaction(cosmosAsyncContainer);
            testFunction.accept(cosmosAsyncContainer);
        } finally {
            try {
                deleteTransaction(cosmosAsyncContainer);
            } catch (Exception e) {
                LOG.warn("Deleting transaction failed", e);
            }
            CosmosConfig.closeCosmosAsyncClient(cosmosAsyncClient);
        }
    }

    private static void createTransaction(CosmosAsyncContainer cosmosAsyncContainer) {
        Transaction transaction = new Transaction();
        transaction.setId(TRANSACTION_ID);
        transaction.setAmount(BigDecimal.TEN);
        transaction.setCreationDate(CREATION_DATE);
        transaction.setCreationDateTime(OffsetDateTime.of(2025, 5, 27, 15, 23, 44, 0, ZoneOffset.ofHours(4)));

        LOG.debug("Creating transaction: {}", transaction);
        cosmosAsyncContainer.createItem(transaction).block(Duration.ofSeconds(30));
        LOG.debug("Transaction created: {}", transaction);
    }

    private static void deleteTransaction(CosmosAsyncContainer cosmosAsyncContainer) {
        LOG.debug("Deleting transaction: {}", TRANSACTION_ID);
        cosmosAsyncContainer.deleteItem(TRANSACTION_ID, new PartitionKey(TRANSACTION_ID)).block(Duration.ofSeconds(30));
        LOG.debug("Transaction deleted: {}", TRANSACTION_ID);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readTransactionAsMap(CosmosAsyncContainer cosmosAsyncContainer) {
        return cosmosAsyncContainer.readItem(TRANSACTION_ID, new PartitionKey(TRANSACTION_ID), Map.class)
                .map(CosmosItemResponse::getItem)
                .block(Duration.ofSeconds(30));
    }

    private <T> T queryItem(CosmosAsyncContainer cosmosAsyncContainer, String query, Class<T> clazz) {
        return queryItem(cosmosAsyncContainer, query, List.of(), clazz);
    }

    private <T> T queryItem(CosmosAsyncContainer cosmosAsyncContainer, String query, List<SqlParameter> params, Class<T> clazz) {
        return cosmosAsyncContainer.queryItems(new SqlQuerySpec(query, params), clazz)
                .collectList()
                .flatMap(result -> switch (result.size()) {
                    case 0 -> Mono.empty();
                    case 1 -> Mono.just(result.getFirst());
                    default -> Mono.error(new RuntimeException("Incorrect result size. Expected: %s. Actual: %s".formatted(1, result.size())));
                })
                .block(Duration.ofSeconds(30));
    }

}
