//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package Microsoft.KafkaAvailability;

public interface IConsumer
{
    void ConsumeFromTopicPartition(String a_topic, int a_partition) throws Exception;
}
