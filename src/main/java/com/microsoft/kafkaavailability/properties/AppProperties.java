//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package com.microsoft.kafkaavailability.properties;

public class AppProperties
{
    public String sqlConnectionString;
    public boolean reportKafkaIPAvailability;
    public boolean reportKafkaGTMAvailability;
    public String kafkaClusterIP;
    public String kafkaGTMIP;
    public boolean reportToSql;
    public boolean reportToSlf4j;
    public boolean reportToConsole;
    public boolean reportToCsv;
    public boolean reportToJmx;
    public boolean sendProducerAvailability;
    public boolean sendConsumerAvailability;
    public boolean sendProducerLatency;
    public boolean sendIPAvailabilityLatency;
    public boolean sendGTMAvailabilityLatency;
    public boolean sendConsumerLatency;
    public boolean sendProducerTopicLatency;
    public boolean sendConsumerTopicLatency;
    public boolean sendProducerPartitionLatency;
    public boolean sendConsumerPartitionLatency;
    public int producerThreadSleepTime;
    public int leaderInfoThreadSleepTime;
    public int availabilityThreadSleepTime;
    public String csvDirectory;
}
