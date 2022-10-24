package org.example.testtools.client;

/*-
 * ========================LICENSE_START=================================
 * Kafka Synchronisation Connectors for Kafka Connect
 * %%
 * Copyright (C) 2021 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.apache.kafka.connect.runtime.rest.entities.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ConnectClient {

    static ConnectClient createClient(String url) {
        return new ConnectClientImpl(url);
    }

    List<PluginInfo> getAllConnectorPlugins();

    ConfigInfos validateConnectorPluginConfig(String connectorType, Map<String, String> connectorConfig);

    Collection<String> getAllConnectors();

    ConnectorInfo getConnector(String connectorName);

    List<TaskInfo> getConnectorTasks(String connectorName);

    ConnectorStateInfo.TaskState getConnectorTaskState(String connectorName, int taskId);

    void restartConnectorTask(String connectorName, int taskId);

    ConnectorInfo createConnector(String connectorName, Map<String, String> config);

    Map<String, String> getConnectorConfig(String connectorName);

    ConnectorInfo updateConnectorConfig(String connectorName, Map<String, String> config);

    void restartConnector(String connectorName);

    void resumeConnector(String connectorName);

    void pauseConnector(String connectorName);

    void deleteConnector(String connectorName);

    ConnectorStateInfo getConnectorStatus(String connectorName);
}
