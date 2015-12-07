/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.config;

import javax.jms.Connection;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQConfig extends MapConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConfig.class);
    
    public static final String CONFIG_KEY_ACTIVEMQ_BROKER_URL = "broker.url";
    public static final String CONFIG_KEY_ACTIVEMQ_BROKER_USER = "broker.user";
    public static final String CONFIG_KEY_ACTIVEMQ_BROKER_PASSWORD = "broker.password";
    
    public ActiveMQConfig(String name, Config config) {
        super(config.subset("systems." + name + "."));
    }
 
    public String getURL() {
        if (containsKey(CONFIG_KEY_ACTIVEMQ_BROKER_URL)) {
            return get(CONFIG_KEY_ACTIVEMQ_BROKER_URL);
        }
        throw new SamzaException("You must specify the ActiveMQ broker URL.");
    }

    public String getUser() {
        if (containsKey(CONFIG_KEY_ACTIVEMQ_BROKER_USER)) {
            return get(CONFIG_KEY_ACTIVEMQ_BROKER_USER);
        } else {
            return null;
        }
    }

    public String getPassword() {
        if (containsKey(CONFIG_KEY_ACTIVEMQ_BROKER_PASSWORD)) {
            return get(CONFIG_KEY_ACTIVEMQ_BROKER_PASSWORD);
        } else {
            return null;
        }
    }

    public Connection createConnection() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(getURL());
        return connectionFactory.createConnection(getUser(), getPassword());
    }
}