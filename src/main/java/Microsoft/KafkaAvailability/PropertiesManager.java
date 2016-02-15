//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package Microsoft.KafkaAvailability;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/***
 * Gets property values from json files.
 * @param <T> T is the type used to serialize and deserialize the json file.
 */
public class PropertiesManager<T> implements IPropertiesManager<T>
{
    private String m_propFileName;
    private T m_prop;
    final Class<T> m_typeParameterClass;

    /***
     *
     * @param propFileName json file containing properties
     * @param typeParameterClass The class object associated with the type T
     * @throws IOException if property file is not found in classpath
     */
    public PropertiesManager(String propFileName, Class<T> typeParameterClass) throws IOException
    {
        this.m_propFileName = propFileName;
        m_typeParameterClass = typeParameterClass;
        Gson gson = new Gson();
        URL url = Thread.currentThread().getContextClassLoader().getResource(propFileName);

        if (url != null)
        {
            String text = Resources.toString(url, Charsets.UTF_8);
            m_prop = gson.fromJson(text, m_typeParameterClass);
        } else
        {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
    }

    /***
     *
     * @return An object of the type T that contains the properties from the json file.
     */
    public T getProperties()
    {
        return m_prop;
    }
}
