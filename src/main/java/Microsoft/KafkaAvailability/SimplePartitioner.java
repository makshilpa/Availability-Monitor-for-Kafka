//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package Microsoft.KafkaAvailability;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner
{
    public SimplePartitioner(VerifiableProperties props)
    {
    }

    /***
     *
     * @param key partitioning key
     * @param a_numPartitions total number of partitions
     * @return partition number to which this message will be sent
     */
    public int partition(Object key, int a_numPartitions)
    {
        return Integer.parseInt(key.toString()) % a_numPartitions;
    }
}