//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import java.io.IOException;

public interface IProducer
{
    void SendCanaryToTopicPartition(String topicName, String partitionId);
    void SendCanaryToKafkaIP(String kafkaIP, String topicName, boolean enableCertCheck) throws Exception;
    void close() throws IOException;
}
