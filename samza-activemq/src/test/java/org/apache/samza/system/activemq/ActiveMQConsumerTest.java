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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ActiveMQConsumerTest {

    private ActiveMQConsumer consumer;
    
    @Test
    public void testPoll() throws JMSException, InterruptedException {
        consumer = new ActiveMQConsumer("activemq", ActiveMQTestHelper.createConsumerConfig(false), new NoOpMetricsRegistry());
        final SystemStreamPartition sspRequest = new SystemStreamPartition("activemq", "test-queue", new Partition(0));
        consumer.register(new SystemStreamPartition("activemq", "test-queue", new Partition(0)), "");
        consumer.start();
        
        final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResponse = consumer.poll(Collections.singleton(sspRequest), 100l);
        assertNotNull(pollResponse);
        assertEquals(1, pollResponse.size());
        final SystemStreamPartition sspResponse = pollResponse.keySet().iterator().next();
        assertEquals(sspRequest, sspResponse);
        final List<IncomingMessageEnvelope> envelopes = pollResponse.get(sspResponse);
        assertEquals(1, envelopes.size());
        final IncomingMessageEnvelope envelope = envelopes.get(0);
        assertEquals("activemq-key", envelope.getKey());
        assertEquals("activemq-message", envelope.getMessage());
    }
    
    @Test
    public void testExceptionDuringPoll() throws JMSException, InterruptedException {
        consumer = new ActiveMQConsumer("activemq", ActiveMQTestHelper.createConsumerConfig(true), new NoOpMetricsRegistry());
        final SystemStreamPartition sspRequest = new SystemStreamPartition("activemq", "test-queue", new Partition(0));
        consumer.register(new SystemStreamPartition("activemq", "test-queue", new Partition(0)), "");
        consumer.start();
        final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResponse = consumer.poll(Collections.singleton(sspRequest), 4);
        assertNotNull(pollResponse);
        assertEquals(0, pollResponse.size());
    }   
}