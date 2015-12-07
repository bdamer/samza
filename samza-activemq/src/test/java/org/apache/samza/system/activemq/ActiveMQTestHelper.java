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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.samza.config.ActiveMQConfig;
import org.mockito.Mockito;

/**
 * Utility class to generate mock ActiveMQ objects for unit tests.
 */
public class ActiveMQTestHelper {

    public static MessageConsumer consumer;
    public static MessageProducer producer;
    
    public static ActiveMQConfig createConsumerConfig(boolean throwException) throws JMSException {
        Queue queue = Mockito.mock(Queue.class);
        
        TextMessage message = Mockito.mock(TextMessage.class);
        Mockito.when(message.getJMSMessageID()).thenReturn("activemq-key");
        Mockito.when(message.getText()).thenReturn("activemq-message");
        
        consumer = Mockito.mock(MessageConsumer.class);
        
        if (throwException) {
            Mockito.when(consumer.receive()).thenThrow(new JMSException("Generic Error"));
        } else {
            Mockito.when(consumer.receive()).thenReturn(message).thenThrow(new JMSException("Generic Error"));  
        }
        
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.createQueue(Mockito.anyString())).thenReturn(queue);
        Mockito.when(session.createConsumer(queue)).thenReturn(consumer);
        
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createSession(Mockito.anyBoolean(), Mockito.anyInt())).thenReturn(session);
        
        ActiveMQConfig config = Mockito.mock(ActiveMQConfig.class);
        Mockito.when(config.createConnection()).thenReturn(connection);
        
        return config;
    }
        
    public static ActiveMQConfig createProducerConfig() throws JMSException {
        Queue queue = Mockito.mock(Queue.class);
        
        producer = Mockito.mock(MessageProducer.class);
        
        Session session = Mockito.mock(Session.class);
        Mockito.when(session.createQueue(Mockito.anyString())).thenReturn(queue);
        Mockito.when(session.createProducer(queue)).thenReturn(producer);
        Mockito.when(session.createBytesMessage()).thenReturn(new ActiveMQBytesMessage());
        Mockito.when(session.createTextMessage()).thenReturn(new ActiveMQTextMessage());
        
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createSession(Mockito.anyBoolean(), Mockito.anyInt())).thenReturn(session);

        ActiveMQConfig config = Mockito.mock(ActiveMQConfig.class);
        Mockito.when(config.createConnection()).thenReturn(connection);
        
        return config;
    }
}