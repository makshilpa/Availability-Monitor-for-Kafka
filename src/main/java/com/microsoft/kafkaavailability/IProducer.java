//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

public interface IProducer
{
    void SendCanaryToTopicPartition(String topicName, String partitionId);
    void SendCanaryToKafkaIP(String kafkaIP, boolean enableCertCheck) throws Exception;
}
