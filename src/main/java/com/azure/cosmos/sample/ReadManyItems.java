// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.ItemOperations;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.implementation.apachecommons.lang.tuple.Pair;
import com.azure.cosmos.implementation.guava25.collect.Lists;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;

public class ReadManyItems {

    private static CosmosAsyncClient client;

    private final String databaseName = "ReadManyItemsDB";
    private final String containerName = "Container4";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    Queue<String> docIDs = new ConcurrentLinkedQueue<String>();
    private static AtomicBoolean resources_created = new AtomicBoolean(false);
    AtomicInteger exceptionCount = new AtomicInteger(0);
    AtomicLong insertCount = new AtomicLong(0);
    AtomicInteger recordCount = new AtomicInteger(0);
    AtomicInteger verifyCount = new AtomicInteger(0);
    AtomicLong totalLatency = new AtomicLong(0);
    AtomicLong totalReadLatency = new AtomicLong(0);
    AtomicLong totalEnhancedReadLatency = new AtomicLong(0);
    AtomicReference<Double> totalRequestCharges = new AtomicReference<>((double) 0);
    AtomicReference<Double> totalEnhancedRequestCharges = new AtomicReference<>((double) 0);
    private static AtomicInteger number_docs_inserted = new AtomicInteger(0);
    private static AtomicInteger number_docs_read = new AtomicInteger(0);

    public static final int NUMBER_OF_DOCS = 1000;
    public static final int NUMBER_OF_DOCS_PER_THREAD = 100;
    public ArrayList<JsonNode> docs;

    public void close() {
        client.close();
    }

    /**
     * Demo readMany method...
     * 
     * @param args command line args.
     */
    // <Main>
    public static void main(String[] args) {
        ReadManyItems p = new ReadManyItems();

        try {
            p.readManyItemsDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            System.out.println("Cosmos getStarted failed with: " + e);
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }
    // </Main>

    private void readManyItemsDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);
        docs = generateDocs(NUMBER_OF_DOCS);
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                // Setting the preferred location to Cosmos DB Account region
                // UK South is just an example. User should set preferred location to the Cosmos
                // DB region closest to the application
                .preferredRegions(Collections.singletonList("UK South"))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();

        Mono<Void> databaseContainerIfNotExist = client.createDatabaseIfNotExists(databaseName)
                .flatMap(databaseResponse -> {
                    database = client.getDatabase(databaseResponse.getProperties().getId());
                    System.out.println("\n\n\n\nCreated database ReadManyItemsDB.\n\n\n\n");
                    CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/id");
                    ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(10000);
                    return database.createContainerIfNotExists(containerProperties, throughputProperties);
                }).flatMap(containerResponse -> {
                    container = database.getContainer(containerResponse.getProperties().getId());
                    System.out.println("\n\n\n\nCreated container Container.\n\n\n\n");
                    return Mono.empty();
                });

        System.out.println("Creating database and container asynchronously...");
        databaseContainerIfNotExist.subscribe(voidItem -> {
        }, err -> {
        },
                () -> {
                    System.out.println("Finished creating resources.\n\n");
                    resources_created.set(true);
                });

        while (!resources_created.get()) {
            // do things async while creating resources...
        }

        createManyItems(docs, NUMBER_OF_DOCS, NUMBER_OF_DOCS_PER_THREAD);

        System.out.println("Reading many items....");
        readManyItems();
        System.out.println("Reading many items (enhanced using readMany method)");
        readManyItemsEnhanced();

        System.out.println("Total latency with standard multi-threading: " + totalReadLatency);
        System.out.println("Total latency using readMany method: " + totalEnhancedReadLatency);
        System.out.println(
                "Total request charges with standard multi-threading totalRequestCharges: " + totalRequestCharges);
        System.out.println("Total request charges using readMany method: " + totalEnhancedRequestCharges);
    }

    private void createManyItems(ArrayList<JsonNode> docs, final int noOfDocs,
            final int noOfDocsPerThread) throws Exception {
        final long startTime = System.currentTimeMillis();
        Flux.fromIterable(docs).flatMap(doc -> container.createItem(doc))
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 201) {
                        number_docs_inserted.getAndIncrement();
                    } else
                        System.out.println("WARNING insert status code {} != 201" + itemResponse.getStatusCode());
                    return Mono.empty();
                }).subscribe(); // ...Subscribing to the publisher triggers stream execution.
        System.out.println("Doing other things until async doc inserts complete...");
        while (number_docs_inserted.get() < NUMBER_OF_DOCS) {
        }
        if (number_docs_inserted.get() == NUMBER_OF_DOCS) {
            final long endTime = System.currentTimeMillis();
            final long duration = (endTime - startTime);
            System.out.print("total insert duration time millis: " + duration + "\n");
            this.totalLatency.getAndAdd(duration);
        }

    }

    private void readManyItems() throws InterruptedException {
        // collect the ids that were generated when writing the data.
        List<String> list = new ArrayList<String>();
        for (final JsonNode doc : docs) {
            list.add(doc.get("id").asText());
        }
        List<List<String>> lists = Lists.partition(list, NUMBER_OF_DOCS_PER_THREAD);

        final long startTime = System.currentTimeMillis();
        Flux.fromIterable(lists).flatMapIterable(x -> x)
                .flatMap(id -> container.readItem(id, new PartitionKey(id), JsonNode.class))
                .flatMap(itemResponse -> {
                    if (itemResponse.getStatusCode() == 200) {
                        double requestCharge = itemResponse.getRequestCharge();
                        BinaryOperator<Double> add = (u, v) -> u + v;
                        totalRequestCharges.getAndAccumulate(requestCharge, add);
                        number_docs_read.getAndIncrement();
                    } else
                        System.out.println("WARNING insert status code {} != 200" + itemResponse.getStatusCode());
                    return Mono.empty();
                }).subscribe();
        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);
        totalReadLatency.getAndAdd(duration);
    }

    private void readManyItemsEnhanced() throws InterruptedException {
        // collect the ids that were generated when writing the data.
        List<String> list = new ArrayList<String>();
        for (final JsonNode doc : docs) {
            list.add(doc.get("id").asText());
        }
        List<List<String>> lists = Lists.partition(list, NUMBER_OF_DOCS_PER_THREAD);

        final long startTime = System.currentTimeMillis();
        Flux.fromIterable(lists).flatMap(x -> {

            List<Pair<String, PartitionKey>> pairList = new ArrayList<>();

            // add point reads in this thread as a list to be sent to Cosmos DB
            for (final String id : x) {
                pairList.add(Pair.of(String.valueOf(id), new PartitionKey(String.valueOf(id))));
            }

            // instead of reading sequentially, send CosmosItem id and partition key tuple
            // of items to be read
            Mono<FeedResponse<Item>> documentFeedResponse = ItemOperations.readManyAsync(container,
                    pairList, Item.class);
            double requestCharge = documentFeedResponse.block().getRequestCharge();
            BinaryOperator<Double> add = (u, v) -> u + v;
            totalEnhancedRequestCharges.getAndAccumulate(requestCharge, add);
            return documentFeedResponse;
        }).subscribe();
        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);
        totalEnhancedReadLatency.getAndAdd(duration);
    }

    public static ArrayList<JsonNode> generateDocs(int N) {
        ArrayList<JsonNode> docs = new ArrayList<JsonNode>();
        ObjectMapper mapper = Utils.getSimpleObjectMapper();

        try {
            for (int i = 1; i <= N; i++) {
                docs.add(mapper.readTree(
                        "{" +
                                "\"id\": " +
                                "\"" + UUID.randomUUID().toString() + "\"" +
                                "}"));
            }
        } catch (Exception err) {
            System.out.println("Failed generating documents: " + err);
        }

        return docs;
    }
}
