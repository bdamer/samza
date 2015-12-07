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

import java.io.UnsupportedEncodingException;
import java.util.List;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ActiveMQConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ActiveMQProducerTest {
    
    private ActiveMQProducer producer;
    private ActiveMQProducerMetrics metrics;
    
    @Before
    public void init() {
        metrics = new ActiveMQProducerMetrics("activemq", new NoOpMetricsRegistry());
    }
    
    @Test
    public void testSend() throws JMSException, UnsupportedEncodingException {
        ActiveMQConfig config = ActiveMQTestHelper.createProducerConfig();
        producer = new ActiveMQProducer("activemq", config, metrics);
        producer.register("junit");
        producer.start();

        final SystemStreamPartition sspRequest = new SystemStreamPartition("activemq", "test-queue", new Partition(0));

        byte[] expected1 = "active mq test".getBytes("utf-8");
        OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(sspRequest, expected1);
        producer.send("junit", envelope);

        String expected2 = "active mq test";
        envelope = new OutgoingMessageEnvelope(sspRequest, expected2);
        producer.send("junit", envelope);
        
        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(ActiveMQTestHelper.producer, Mockito.times(2)).send(captor.capture());

        List<Message> messages = captor.getAllValues();

        Message msg = messages.get(0);
        Assert.assertTrue(msg instanceof BytesMessage);
        // Need to simulate sending message to store data in correct buffer
        ActiveMQBytesMessage bm = (ActiveMQBytesMessage)msg;
        bm.onSend();
        byte[] actual1 = new byte[(int)bm.getBodyLength()];
        bm.readBytes(actual1);
        Assert.assertArrayEquals(expected1, actual1);
        
        msg = messages.get(1);
        Assert.assertTrue(msg instanceof TextMessage);
        String actual2 = ((TextMessage)msg).getText();
        Assert.assertEquals(expected2, actual2);
    }
    
    @Test(expected=SamzaException.class)
    public void testSendInvalidType() throws JMSException {
        ActiveMQConfig config = ActiveMQTestHelper.createProducerConfig();
        producer = new ActiveMQProducer("activemq", config, metrics);
        producer.register("junit");
        producer.start();

        Integer expected = 100;
        final SystemStreamPartition sspRequest = new SystemStreamPartition("activemq", "test-queue", new Partition(0));
        OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(sspRequest, expected);
        producer.send("junit", envelope);
    }
    
    @Ignore
    @Test
    public void testInvalidDestination() throws UnsupportedEncodingException, JMSException {
        ActiveMQConfig config = ActiveMQTestHelper.createProducerConfig();
        producer = new ActiveMQProducer("activemq", config, metrics);
        producer.register("junit");
        producer.start();

        byte[] expected = "active mq test".getBytes("utf-8");
        
        final SystemStreamPartition sspRequest = new SystemStreamPartition("activemq", "invalid-queue", new Partition(0));
        OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(sspRequest, expected);
        producer.send("junit", envelope);

        ArgumentCaptor<Message> captor = ArgumentCaptor.forClass(Message.class);
        Mockito.verify(ActiveMQTestHelper.producer, Mockito.never()).send(captor.capture());
    }
}
