//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.SqlReporter;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ConsumerProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ProducerProperties;
import com.google.gson.Gson;
import org.apache.commons.cli.*;
import org.apache.log4j.MDC;
import org.apache.log4j.net.SyslogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.x.discovery.ServiceInstance;
import com.microsoft.kafkaavailability.discovery.Constants;
import com.microsoft.kafkaavailability.discovery.CuratorManager;
import com.microsoft.kafkaavailability.discovery.CuratorClient;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.discovery.MetaData;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.List;

/***
 * Sends a canary message to every topic and partition in Kafka.
 * Reads data from the tail of every topic and partition in Kafka
 * Reports the availability and latency metrics for the above operations.
 * Availability is defined as the percentage of total partitions that respond to each operation.
 */
public class App {
    final static Logger m_logger = LoggerFactory.getLogger(App.class);
    static int m_sleepTime = 60000;
    static String m_cluster = "localhost";
    static MetricRegistry m_metrics;
    static AppProperties appProperties;
    static MetaDataManagerProperties metaDataProperties;
    static Collection<ServiceInstance<MetaData>> instances;
    static List<String> listServers;

    private static String registrationPath = Constants.DEFAULT_REGISTRATION_ROOT;
    private static String ip = CommonUtils.getIpAddress();
    private static String serviceSpec = "";


    public static void main(String[] args) throws IOException, MetaDataManagerException, InterruptedException {
        m_logger.info("Starting KafkaAvailability Tool");
        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        appProperties = (AppProperties) appPropertiesManager.getProperties();
        metaDataProperties = (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();
        Options options = new Options();
        options.addOption("r", "run", true, "Number of runs. Don't use this argument if you want to run infintely.");
        options.addOption("s", "sleep", true, "Time (in milliseconds) to sleep between each run. Default is 120000");
        Option clusterOption = Option.builder("c").hasArg().required(true).longOpt("cluster").desc("(REQUIRED) Cluster name").build();
        options.addOption(clusterOption);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        final CuratorFramework curatorFramework = CuratorClient.getCuratorFramework(metaDataProperties.zooKeeperHosts);

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            int howManyRuns;

            m_cluster = line.getOptionValue("cluster");
            MDC.put("cluster", m_cluster);
            CuratorManager curatorManager = CallRegister(curatorFramework);

            if (line.hasOption("sleep")) {
                m_sleepTime = Integer.parseInt(line.getOptionValue("sleep"));
            }

            if (line.hasOption("run")) {
                howManyRuns = Integer.parseInt(line.getOptionValue("run"));
                for (int i = 0; i < howManyRuns; i++) {
                    InitMetrics(m_sleepTime);
                    waitForChanges(curatorManager);
                    RunOnce(curatorFramework);
                    Thread.sleep(m_sleepTime);
                }
            } else {
                while (true) {
                    InitMetrics(m_sleepTime);
                    waitForChanges(curatorManager);
                    RunOnce(curatorFramework);
                    Thread.sleep(m_sleepTime);
                }
            }
        } catch (ParseException exp) {
            // oops, something went wrong
            m_logger.error("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("KafkaAvailability", options);
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }

    }


    private static CuratorManager CallRegister(final CuratorFramework curatorFramework) throws Exception {
        int port = ((int) (65535 * Math.random()));
        serviceSpec = ip + ":" + Integer.valueOf(port).toString();

        String basePath = new StringBuilder().append(registrationPath).toString();
        m_logger.info("Creating client, KAT");

        final CuratorManager curatorManager = new CuratorManager(curatorFramework, basePath, ip, serviceSpec);

        try {
            curatorManager.registerLocalService();

            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    m_logger.info("Normal shutdown executing.");
                    curatorManager.unregisterService();
                    if (curatorFramework != null && (curatorFramework.getState().equals(CuratorFrameworkState.STARTED) || curatorFramework.getState().equals(CuratorFrameworkState.LATENT))) {
                        curatorFramework.close();
                    }
                }
            });
        } catch (Exception e) {
            m_logger.error(e.getMessage(), e);
        }
        return curatorManager;
    }

    private static void waitForChanges(CuratorManager curatorManager) throws Exception {

        try {
            listServers = curatorManager.listServiceInstance();
            m_logger.info("listServers:" + Arrays.toString(listServers.toArray()));

            //wait for rest clients to warm up.
            Thread.sleep(30000);

            curatorManager.verifyRegistrations();
        } catch (Exception e) {
                /*                 * Something bad did happen, but carry on
                 */
            m_logger.error(e.getMessage(), e);
        }
    }

    private static void InitMetrics(int reportDuration) {
        m_metrics = new MetricRegistry();

        if (appProperties.reportToSlf4j) {
            final Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(m_metrics)
                    .outputTo(LoggerFactory.getLogger("KafkaMetrics.Raw"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            slf4jReporter.start(reportDuration, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToSql) {
            final SqlReporter sqlReporter = SqlReporter.forRegistry(m_metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(appProperties.sqlConnectionString, m_cluster);
            sqlReporter.start(reportDuration, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToJmx) {
            final JmxReporter jmxReporter = JmxReporter.forRegistry(m_metrics).build();
            jmxReporter.start();
        }
        if (appProperties.reportToConsole) {
            final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(m_metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            consoleReporter.start(reportDuration, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToCsv) {
            final CsvReporter csvReporter = CsvReporter.forRegistry(m_metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(appProperties.csvDirectory));
            csvReporter.start(reportDuration, TimeUnit.MILLISECONDS);
        }
        //return m_metrics;
    }


    private static void RunOnce(CuratorFramework curatorFramework) throws IOException, MetaDataManagerException {
        IPropertiesManager producerPropertiesManager = new PropertiesManager<ProducerProperties>("producerProperties.json", ProducerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IPropertiesManager consumerPropertiesManager = new PropertiesManager<ConsumerProperties>("consumerProperties.json", ConsumerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(curatorFramework, metaDataPropertiesManager);
        IProducer producer = new Producer(producerPropertiesManager, metaDataManager);
        IConsumer consumer = new Consumer(consumerPropertiesManager, metaDataManager);

        int producerTryCount = 0, clusterIPStatusTryCount = 0, gtmIPStatusTryCount = 0;
        int producerFailCount = 0, clusterIPStatusFailCount = 0, gtmIPStatusFailCount = 0;
        long startTime, endTime;
        int numPartitionsProducer = 0, numPartitionsConsumers = 0;
        //Auto creating a whitelisted topics, if not available.
        metaDataManager.createWhiteListedTopics();
        m_logger.info("get metadata size");

        //This is full list of topics
        List<kafka.javaapi.TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();

        //Collections.sort(totalTopicMetadata, new TopicMetadataComparator());

        List<kafka.javaapi.TopicMetadata> whiteListTopicMetadata = new ArrayList<kafka.javaapi.TopicMetadata>();
        List<kafka.javaapi.TopicMetadata> allTopicMetadata = new ArrayList<kafka.javaapi.TopicMetadata>();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            for (String whiteListTopic : metaDataProperties.topicsWhitelist)
                // java string compare while ignoring case
                if (topic.topic().equalsIgnoreCase(whiteListTopic)) {
                    whiteListTopicMetadata.add(topic);
                }
            if ((totalTopicMetadata.indexOf(topic) % listServers.size()) == listServers.indexOf(serviceSpec)) {
                allTopicMetadata.add(topic);
            }
        }

        m_logger.info("totalTopicMetadata size:" + totalTopicMetadata.size());
        m_logger.info("whiteListTopicMetadata size:" + whiteListTopicMetadata.size());
        m_logger.info("allTopicMetadata size:" + allTopicMetadata.size());

        m_logger.info("Starting ProducerLatency");

        m_logger.info("done getting metadata size for producer");
        for (kafka.javaapi.TopicMetadata topic : whiteListTopicMetadata) {
            numPartitionsProducer += topic.partitionsMetadata().size();
        }

        final SlidingWindowReservoir producerLatencyWindow = new SlidingWindowReservoir(numPartitionsProducer);
        Histogram histogramProducerLatency = new Histogram(producerLatencyWindow);
        MetricNameEncoded producerLatency = new MetricNameEncoded("Producer.Latency", "all");
        if (appProperties.sendProducerLatency)
            m_metrics.register(new Gson().toJson(producerLatency), histogramProducerLatency);
        m_logger.info("start topic partition loop");

        for (kafka.javaapi.TopicMetadata item : whiteListTopicMetadata) {
            m_logger.info("Starting VIP prop check." + appProperties.reportKafkaIPAvailability);
            if (appProperties.reportKafkaIPAvailability) {
                try {
                    m_logger.info("Starting VIP check.");
                    clusterIPStatusTryCount++;
                    producer.SendCanaryToKafkaIP(appProperties.kafkaClusterIP, item.topic(), false);
                } catch (Exception e) {
                    m_logger.info("VIP check exception");
                    clusterIPStatusFailCount++;
                    m_logger.error("ClusterIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                }
                try {
                    gtmIPStatusTryCount++;
                    producer.SendCanaryToKafkaIP(appProperties.kafkaGTMIP, item.topic(), false);
                } catch (Exception e) {
                    gtmIPStatusFailCount++;
                    m_logger.error("GTMIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                }
            }
            m_logger.info("done with VIP prop check.");
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramProducerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded producerTopicLatency = new MetricNameEncoded("Producer.Topic.Latency", item.topic());
            if (!m_metrics.getNames().contains(new Gson().toJson(producerTopicLatency))) {
                if (appProperties.sendProducerTopicLatency)
                    m_metrics.register(new Gson().toJson(producerTopicLatency), histogramProducerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                m_logger.debug("Writing to Topic: {}; Partition: {};", item.topic(), part.partitionId());
                MetricNameEncoded producerPartitionLatency = new MetricNameEncoded("Producer.Partition.Latency", item.topic() + "-" + part.partitionId());
                Histogram histogramProducerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!m_metrics.getNames().contains(new Gson().toJson(producerPartitionLatency))) {
                    if (appProperties.sendProducerPartitionLatency)
                        m_metrics.register(new Gson().toJson(producerPartitionLatency), histogramProducerPartitionLatency);
                }
                startTime = System.currentTimeMillis();
                try {
                    producerTryCount++;
                    producer.SendCanaryToTopicPartition(item.topic(), Integer.toString(part.partitionId()));
                    endTime = System.currentTimeMillis();
                } catch (Exception e) {
                    m_logger.error("Error Writing to Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    producerFailCount++;
<<<<<<< HEAD
                    //now add half an hour, 1 800 000 miliseconds = 30 minutes
=======
                    //now add half an hour, 1 800 000 milliseconds = 30 minutes
>>>>>>> KafkaAvailability_log4j
                    long halfAnHourLater = System.currentTimeMillis() + 1800000;
                    endTime = halfAnHourLater;
                }
                histogramProducerLatency.update(endTime - startTime);
                histogramProducerTopicLatency.update(endTime - startTime);
                histogramProducerPartitionLatency.update(endTime - startTime);
            }
        }
        if (appProperties.reportKafkaIPAvailability) {
            m_logger.info("About to report kafkaIPAvail:" + clusterIPStatusTryCount + " " + clusterIPStatusFailCount);
            MetricNameEncoded kafkaClusterIPAvailability = new MetricNameEncoded("KafkaIP.Availability", "all");
            m_metrics.register(new Gson().toJson(kafkaClusterIPAvailability), new AvailabilityGauge(clusterIPStatusTryCount, clusterIPStatusTryCount - clusterIPStatusFailCount));
            MetricNameEncoded kafkaGTMIPAvailability = new MetricNameEncoded("KafkaGTMIP.Availability", "all");
            m_metrics.register(new Gson().toJson(kafkaGTMIPAvailability), new AvailabilityGauge(gtmIPStatusTryCount, gtmIPStatusTryCount - gtmIPStatusFailCount));
        }
        if (appProperties.sendProducerAvailability) {
            MetricNameEncoded producerAvailability = new MetricNameEncoded("Producer.Availability", "all");
            m_metrics.register(new Gson().toJson(producerAvailability), new AvailabilityGauge(producerTryCount, producerTryCount - producerFailCount));
        }

        int consumerTryCount = 0;
        int consumerFailCount = 0;

        m_logger.info("done getting metadata size for consumer");

        for (kafka.javaapi.TopicMetadata topic : allTopicMetadata) {
            numPartitionsConsumers += topic.partitionsMetadata().size();
        }

        final SlidingWindowReservoir consumerLatencyWindow = new SlidingWindowReservoir(numPartitionsConsumers);
        Histogram histogramConsumerLatency = new Histogram(consumerLatencyWindow);

        if (appProperties.sendConsumerLatency) {
            MetricNameEncoded consumerLatency = new MetricNameEncoded("Consumer.Latency", "all");
            m_metrics.register(new Gson().toJson(consumerLatency), histogramConsumerLatency);
        }
        for (kafka.javaapi.TopicMetadata item : allTopicMetadata) {
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramConsumerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded consumerTopicLatency = new MetricNameEncoded("Consumer.Topic.Latency", item.topic());
            if (!m_metrics.getNames().contains(new Gson().toJson(consumerTopicLatency))) {
                if (appProperties.sendConsumerTopicLatency)
                    m_metrics.register(new Gson().toJson(consumerTopicLatency), histogramConsumerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                m_logger.debug("Reading from Topic: {}; Partition: {};", item.topic(), part.partitionId());
                MetricNameEncoded consumerPartitionLatency = new MetricNameEncoded("Consumer.Partition.Latency", item.topic() + "-" + part.partitionId());
                Histogram histogramConsumerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!m_metrics.getNames().contains(new Gson().toJson(consumerPartitionLatency))) {
                    if (appProperties.sendConsumerPartitionLatency)
                        m_metrics.register(new Gson().toJson(consumerPartitionLatency), histogramConsumerPartitionLatency);
                }

                startTime = System.currentTimeMillis();
                try {
                    consumerTryCount++;
                    consumer.ConsumeFromTopicPartition(item.topic(), part.partitionId());
                    endTime = System.currentTimeMillis();
                } catch (Exception e) {
                    m_logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    consumerFailCount++;

                    //now add half an hour, 1 800 000 milliseconds = 30 minutes
                    long halfAnHourLater = System.currentTimeMillis() + 1800000;
                    endTime = halfAnHourLater;
                    consumerFailCount++;
                }
                histogramConsumerLatency.update(endTime - startTime);
                histogramConsumerTopicLatency.update(endTime - startTime);
                histogramConsumerPartitionLatency.update(endTime - startTime);
            }
        }

        if (appProperties.sendConsumerAvailability) {
            MetricNameEncoded consumerAvailability = new MetricNameEncoded("Consumer.Availability", "all");
            m_metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(consumerTryCount, consumerTryCount - consumerFailCount));
        }

        //Print all the topic/partition information.
        m_logger.info("Printing all the topic/partition information.");
        metaDataManager.printEverything();

        m_logger.info("All Finished.");

        ((MetaDataManager) metaDataManager).close();
    }
}