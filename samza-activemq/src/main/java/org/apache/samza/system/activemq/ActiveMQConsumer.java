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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.samza.config.ActiveMQConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ consumer implementation which receives messages from one or more 
 * queues. This implementation accepts messages of type {@code TextMessage} and
 * {@code BytesMessage}.
 */
public class ActiveMQConsumer extends BlockingEnvelopeMap {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConsumer.class);

    private final String systemName;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Set<SystemStreamPartition> partitions = new HashSet<>();
    private final Connection connection;
    private Session session;
    
    private class QueueConsumer implements Runnable {

        private final MessageConsumer consumer;
        private final SystemStreamPartition partition;
        
        public QueueConsumer(Session session, SystemStreamPartition partition) throws JMSException {
            LOG.debug("Creating queue consumer for: {}", partition.getStream());
            Destination dest = session.createQueue(partition.getStream());
            this.consumer = session.createConsumer(dest);
            this.partition = partition;
        }
        
        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Message msg = consumer.receive();
                        if (msg instanceof TextMessage) {
                            put(partition, new IncomingMessageEnvelope(partition, null, msg.getJMSMessageID(), ((TextMessage)msg).getText()));
                        } else if (msg instanceof BytesMessage) {
                            BytesMessage bm = (BytesMessage)msg;
                            byte[] buffer = new byte[(int)bm.getBodyLength()];
                            bm.readBytes(buffer, (int)bm.getBodyLength());
                            put(partition, new IncomingMessageEnvelope(partition, null, msg.getJMSMessageID(), buffer));
                        } else {
                            LOG.warn("Unsupported message type: {}", msg.getJMSType());
                        }
                    } catch (InterruptedException e) {
                        LOG.info("Interrupt from Java");
                        Thread.currentThread().interrupt();
                    } catch (JMSException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            LOG.info("Interrupt from JMS");
                            Thread.currentThread().interrupt();
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Error consuming messages.", t);
            } finally {
                try {
                    consumer.close();
                } catch (JMSException ex) {
                    LOG.warn("Could not close queue consumer.", ex);
                }
            }         
        }
    };
    
    public ActiveMQConsumer(String systemName, ActiveMQConfig config, MetricsRegistry metricsRegistry) throws JMSException {
        super(metricsRegistry);
        this.systemName = systemName;
        connection = config.createConnection();
        connect();
    }

    private void connect() throws JMSException {
        LOG.debug("Connecting to ActiveMQ broker.");
        if (session != null) {
            session.close();
        }
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
        LOG.info("Registering stream partition '{}' with system {}", systemStreamPartition.getStream(), 
                systemStreamPartition.getSystem());
        if (partitions.contains(systemStreamPartition)) {
            LOG.warn("Stream partition {} is already registered!", systemStreamPartition.getStream());
            return;
        }
        super.register(systemStreamPartition, startingOffset);
        partitions.add(systemStreamPartition);
    }
    
    @Override
    public void start() {
        LOG.info("Starting consumer for: {}", systemName);
        try {
            for (SystemStreamPartition p : partitions) {
                executor.submit(new QueueConsumer(session, p));
            }
        } catch (JMSException e) {
            LOG.error("Could not start ActiveMQ consumer.", e);
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping consumer for: {}", systemName);
        try {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.stop();
            }
        } catch (InterruptedException | JMSException e) {
            LOG.error("Error stopping ActiveMQ consumer.", e);
        }
    }
}