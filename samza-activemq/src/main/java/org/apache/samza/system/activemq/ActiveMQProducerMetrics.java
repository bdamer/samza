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

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsBase;
import org.apache.samza.metrics.MetricsRegistry;

public class ActiveMQProducerMetrics extends MetricsBase {
    
    public final Counter sendSuccess;
    public final Counter sendErrors;
    
    public ActiveMQProducerMetrics(String prefix, MetricsRegistry registry) {
        super(prefix + "-producer-", registry);
        sendSuccess = new Counter("send-message-success-count");
        sendErrors = new Counter("send-message-error-count");
    }
}