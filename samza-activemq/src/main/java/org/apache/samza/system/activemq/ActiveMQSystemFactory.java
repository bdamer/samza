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
package org.apache.samza.system.activemq;

import javax.jms.JMSException;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ActiveMQConfig;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

public class ActiveMQSystemFactory implements SystemFactory {

    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        try {
            return new ActiveMQConsumer(systemName, new ActiveMQConfig(systemName, config), registry);
        } catch (JMSException ex) {
            throw new SamzaException("Could not create ActiveMQ consumer.", ex);
        }
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        try {
            return new ActiveMQProducer(systemName, new ActiveMQConfig(systemName, config), new ActiveMQProducerMetrics(systemName, registry));
        } catch (JMSException ex) {
            throw new SamzaException("Could not create ActiveMQ producer.", ex);
        }
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new ActiveMQAdmin();
    }
}