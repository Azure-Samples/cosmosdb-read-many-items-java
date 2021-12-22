// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.ItemOperations;
import com.azure.cosmos.implementation.apachecommons.lang.tuple.Pair;
import com.azure.cosmos.implementation.guava25.collect.Lists;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Item;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;

public class ReadManyItems {

    private CosmosClient client;

    private final String databaseName = "ReadManyItemsDB";
    private final String containerName = "Container";

    private CosmosDatabase database;
    private CosmosContainer container;

    Queue<String> docIDs = new ConcurrentLinkedQueue<String>();
    AtomicInteger exceptionCount = new AtomicInteger(0);
    AtomicLong insertCount = new AtomicLong(0);
    AtomicInteger recordCount = new AtomicInteger(0);
    AtomicInteger verifyCount = new AtomicInteger(0);
    AtomicLong totalLatency = new AtomicLong(0);
    AtomicLong totalReadLatency = new AtomicLong(0);
    AtomicLong totalEnhancedReadLatency = new AtomicLong(0);
    AtomicReference<Double> totalRequestCharges = new AtomicReference<>((double) 0);
    AtomicReference<Double> totalEnhancedRequestCharges = new AtomicReference<>((double) 0);

    public static final int NUMBER_OF_THREADS = 250;
    public static final int NUMBER_OF_WRITES_PER_THREAD = 2;

    public void close() {
        client.close();
    }

    /**
     * Demo readMany method...
     * @param args command line args.
     */
    // <Main>
    public static void main(String[] args) {
        ReadManyItems p = new ReadManyItems();

        try {
            p.readManyItemsDemo();
            System.out.println("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            System.out.println("Cosmos getStarted failed with: "+ e);
        } finally {
            System.out.println("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    // </Main>

    private void readManyItemsDemo() throws Exception {
        System.out.println("Using Azure Cosmos DB endpoint: " +AccountSettings.HOST);

        // Create sync client
        // <CreateSyncClient>
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                // Setting the preferred location to Cosmos DB Account region
                // West US is just an example. User should set preferred location to the Cosmos
                // DB region closest to the application
                .preferredRegions(Collections.singletonList("UK South"))
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .buildClient();

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //generate records to be read later by the two different methods
        createManyItems(NUMBER_OF_THREADS,NUMBER_OF_WRITES_PER_THREAD);

        //compare multi-threaded scatter/gather with using readMany
        System.out.println("Reading many items....");
        readManyItems();
        System.out.println("Reading many items (enhanced using readMany method)");
        readManyItemsEnhanced();

        System.out.println("Total latency with standard multi-threading: "+totalReadLatency);
        System.out.println("Total latency using readMany method: "+totalEnhancedReadLatency);
        System.out.println("Total request charges with standard multi-threading: "+totalRequestCharges);
        System.out.println("Total request charges using readMany method: "+totalEnhancedRequestCharges);
    }

    private void createDatabaseIfNotExists() throws Exception {
        CosmosDatabaseResponse cosmosDatabaseResponse = client.createDatabaseIfNotExists(databaseName);
        database = client.getDatabase(cosmosDatabaseResponse.getProperties().getId());
    }

    private void createContainerIfNotExists() throws Exception {
        CosmosContainerProperties containerProperties = new CosmosContainerProperties(containerName, "/pk");
        // Create container with 10000 RU/s
        CosmosContainerResponse cosmosContainerResponse = database.createContainerIfNotExists(containerProperties,
                ThroughputProperties.createManualThroughput(10000));
        container = database.getContainer(cosmosContainerResponse.getProperties().getId());
    }

    private void createManyItems(final int noOfThreads,
    final int noOfWritesPerThread) throws Exception {
        final ExecutorService es = Executors.newCachedThreadPool();
        final long startTime = System.currentTimeMillis();
        for (int i = 1; i <= noOfThreads; i++) {
            final Runnable task = () -> {
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    final UUID guid = java.util.UUID.randomUUID();
                    final String strGuid = guid.toString();
                    //add the doc id to a global list to be used later for testing read performance.
                    this.docIDs.add(strGuid);
                    try {
                        recordCount.incrementAndGet();                       
                        Item item = new Item();
                        item.setId(strGuid);
                        item.setPk(strGuid);
                        CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
                        container.createItem(item, new PartitionKey(item.getPk()), cosmosItemRequestOptions);
                        //createItem(item);                                                
                        insertCount.incrementAndGet();
                    } catch (final Exception e) {
                        exceptionCount.incrementAndGet();
                        System.out.println("Exception: " + e);
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();
        final boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        final long endTime = System.currentTimeMillis();
        final long duration = (endTime - startTime);
        System.out.print("total insert duration time millis: " + duration + "\n");
        this.totalLatency.getAndAdd(duration);
        if (finished) {
            Thread.sleep(1000);
        }        

    }

    private void readManyItems() throws InterruptedException {
        //collect the ids that were generated when writing the data.
        List<String> list = new ArrayList<String>();
        for (final String id : this.docIDs) {
            list.add(id);
        }
        final ExecutorService es = Executors.newCachedThreadPool();
        List<List<String>> lists = Lists.partition(list, NUMBER_OF_WRITES_PER_THREAD);
        final long totalStartTime = System.currentTimeMillis();
        for (List<String> splitList : lists) {
            final Runnable task = () -> {
                for (final String id : splitList) {
                    try {
                        try {
                            //read each item in this chunk sequentially
                            CosmosItemResponse<Item> item = container.readItem(id, new PartitionKey(id), Item.class);
                            double requestCharge = item.getRequestCharge(); 
                            BinaryOperator<Double> add
                            = (u, v) -> u + v;
                            totalRequestCharges.getAndAccumulate(requestCharge, add);
                        
                        } catch (CosmosException e) {
                            System.out.println("Read Item failed with"+e);
                        }
                    } catch (final Exception e) {
                        exceptionCount.incrementAndGet();
                        System.out.println("Exception: " + e);
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();
        final boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        final long totalEndTime = System.currentTimeMillis();
        final long totalDuration = (totalEndTime - totalStartTime);        
        totalReadLatency.getAndAdd(totalDuration);
        if (finished) {
            Thread.sleep(1000);
        }
    }

    private void readManyItemsEnhanced() throws InterruptedException {
        //collect the ids that were generated when writing the data.
        List<String> list = new ArrayList<String>();
        for (final String id : this.docIDs) {
            list.add(id);
        }
        final ExecutorService es = Executors.newCachedThreadPool();
        List<List<String>> lists = Lists.partition(list, NUMBER_OF_WRITES_PER_THREAD);
        final long totalStartTime = System.currentTimeMillis();
        for (List<String> splitList : lists) {
            final Runnable task = () -> {
                List<Pair<String, PartitionKey>> pairList = new ArrayList<>();

                //add point reads in this thread as a list to be sent to Cosmos DB 
                for (final String id : splitList) {
                    pairList.add(Pair.of(String.valueOf(id), new PartitionKey(String.valueOf(id))));
                }

                //instead of reading sequentially, send CosmosItem id and partition key tuple of items to be read
                FeedResponse<Item> documentFeedResponse = ItemOperations.readMany(container, pairList, Item.class);
                double requestCharge = documentFeedResponse.getRequestCharge(); 
                BinaryOperator<Double> add
                = (u, v) -> u + v;
                totalEnhancedRequestCharges.getAndAccumulate(requestCharge, add);                               
            };
            es.execute(task);
        }
        es.shutdown();
        final boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        final long totalEndTime = System.currentTimeMillis();
        final long totalDuration = (totalEndTime - totalStartTime);
        totalEnhancedReadLatency.getAndAdd(totalDuration);
        if (finished) {
            Thread.sleep(1000);
        }
    }    
}
