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

import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ActiveMQConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ producer implementation which sends messages to one or more 
 * queues. This implementation accepts strings and binary messages.  
 */
public class ActiveMQProducer implements SystemProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQProducer.class);
    
    private final String systemName;
    private final ActiveMQProducerMetrics metrics;
    private final Connection connection;
    private final Session session;
    private final Map<String,MessageProducer> producers = new HashMap<>();
    
    public ActiveMQProducer(String systemName, ActiveMQConfig config, ActiveMQProducerMetrics metrics) throws JMSException {
        this.systemName = systemName;
        this.metrics = metrics;
        connection = config.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    
    @Override
    public void start() {
        LOG.info("Starting producer for: {}", systemName);
    }

    @Override
    public void stop() {
        LOG.info("Stopping producer for: {}", systemName);
        try {
            for (MessageProducer producer : producers.values()) {
                producer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.stop();
            }
        } catch (JMSException ex) {
            LOG.error("Error stopping ActiveMQ producer.", ex);
        }
    }

    @Override
    public void register(String source) {
        LOG.info("Registering source {} with system {}", source, systemName);
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
        try {
            String stream = envelope.getSystemStream().getStream();
            MessageProducer producer = producers.get(stream);
            if (producer == null) {
                LOG.info("Creating producer for: {}", stream);
                Destination destination = session.createQueue(stream);
                producer = session.createProducer(destination);
                producers.put(stream, producer);
            }
            Object payload = envelope.getMessage();
            if (payload instanceof byte[]) {
                BytesMessage msg = session.createBytesMessage();
                msg.writeBytes((byte[])payload);
                producer.send(msg);
            } else if (payload instanceof String) {
                TextMessage msg = session.createTextMessage();
                msg.setText((String)payload);
                producer.send(msg);
            } else {
                throw new SamzaException("Unsupported object type: " + payload.getClass());
            }
            metrics.sendSuccess.inc();
        } catch (JMSException e) {
            LOG.error("Failed to send message", e);
            metrics.sendErrors.inc();
        }
    }

    @Override
    public void flush(String source) {
        LOG.info("Flush not supported for ActiveMQ.");
    }
}