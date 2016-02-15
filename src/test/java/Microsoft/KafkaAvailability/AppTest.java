

//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package Microsoft.KafkaAvailability;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import kafka.utils.VerifiableProperties;

import java.io.IOException;

public class AppTest
        extends TestCase
{

    public AppTest(String testName)
    {
        super(testName);
    }


    public static Test suite()
    {
        return new TestSuite(AppTest.class);
    }


    public void testSimplePartitioner()
    {
        SimplePartitioner simplePartitioner = new SimplePartitioner(new VerifiableProperties());
        assertTrue(simplePartitioner.partition(0, 2) == 0);
        assertTrue(simplePartitioner.partition(1, 2) == 1);
        assertTrue(simplePartitioner.partition(2, 2) == 0);
    }

    public void testPropertiesManager() throws IOException
    {
        IPropertiesManager<TestProperties> propManager = new PropertiesManager<TestProperties>("testProperties.json", TestProperties.class);
        assertTrue(propManager.getProperties().serializer_class.equals("kafka.serializer.StringEncoder"));
    }
}
