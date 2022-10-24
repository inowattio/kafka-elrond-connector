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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.rest.entities.*;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Modified version of Apache Kafka RestClient to get data using Connect Rest interface
 */
public class ConnectClientImpl implements ConnectClient {
    private static final Logger log = LoggerFactory.getLogger(ConnectClientImpl.class);
    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    static {
        JSON_SERDE.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        JSON_SERDE.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_VALUES);
        JSON_SERDE.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);
    }

    public final String url;

    ConnectClientImpl(String url) {
        this.url = url.replaceAll("/$", "");
    }

    @Override
    public List<PluginInfo> getAllConnectorPlugins() {
        final String completeUrl = url + "/connector-plugins";
        HttpResponse<List<PluginInfo>> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<List<PluginInfo>>() {
                });
        return response.body;
    }

    @Override
    public ConfigInfos validateConnectorPluginConfig(String connectorType, Map<String, String> connectorConfig) {
        final String completeUrl = String.format("%s/connector-plugins/%s/config/validate", url, connectorType);

        HttpResponse<ConfigInfos> response = httpRequest(
                completeUrl,
                HttpMethod.PUT,
                connectorConfig,
                new TypeReference<ConfigInfos>() {
                }
        );
        return response.body;
    }

    @Override
    public Collection<String> getAllConnectors() {
        final String completeUrl = String.format("%s/connectors", url);
        HttpResponse<Collection<String>> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<Collection<String>>() {
                }
        );
        return response.body;
    }

    @Override
    public ConnectorInfo getConnector(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s", url, connectorName);
        HttpResponse<ConnectorInfo> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<ConnectorInfo>() {
                }
        );
        return response.body;
    }

    @Override
    public List<TaskInfo> getConnectorTasks(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/tasks", url, connectorName);
        HttpResponse<List<TaskInfo>> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<List<TaskInfo>>() {
                }
        );
        return response.body;
    }

    @Override
    public ConnectorStateInfo.TaskState getConnectorTaskState(String connectorName, int taskId) {
        final String completeUrl = String.format("%s/connectors/%s/tasks/%d/status", url, connectorName, taskId);
        HttpResponse<ConnectorStateInfo.TaskState> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<ConnectorStateInfo.TaskState>() {
                }
        );
        return response.body;
    }

    @Override
    public void restartConnectorTask(String connectorName, int taskId) {
        final String completeUrl = String.format("%s/connectors/%s/tasks/%d/restart", url, connectorName, taskId);
        HttpResponse<Void> response = httpRequest(
                completeUrl,
                HttpMethod.POST,
                null,
                new TypeReference<Void>() {
                }
        );
    }

    @Override
    public ConnectorInfo createConnector(String connectorName, Map<String, String> config) {
        final String completeUrl = String.format("%s/connectors", url);
        HttpResponse<ConnectorInfo> response = httpRequest(
                completeUrl,
                HttpMethod.POST,
                new CreateConnectorRequest(connectorName, config),
                new TypeReference<ConnectorInfo>() {
                }
        );
        return response.body;
    }

    @Override
    public Map<String, String> getConnectorConfig(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/config", url, connectorName);
        HttpResponse<Map<String, String>> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<Map<String, String>>() {
                }
        );
        return response.body;
    }

    @Override
    public ConnectorInfo updateConnectorConfig(String connectorName, Map<String, String> config) {
        final String completeUrl = String.format("%s/connectors/%s/config", url, connectorName);
        HttpResponse<ConnectorInfo> response = httpRequest(
                completeUrl,
                HttpMethod.PUT,
                config,
                new TypeReference<ConnectorInfo>() {
                }
        );
        return response.body;
    }

    @Override
    public void restartConnector(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/restart", url, connectorName);
        HttpResponse<Void> response = httpRequest(
                completeUrl,
                HttpMethod.POST,
                null,
                new TypeReference<Void>() {
                }
        );
    }

    @Override
    public void resumeConnector(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/resume", url, connectorName);
        HttpResponse<Void> response = httpRequest(
                completeUrl,
                HttpMethod.PUT,
                null,
                new TypeReference<Void>() {
                }
        );
    }

    @Override
    public void pauseConnector(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/pause", url, connectorName);
        HttpResponse<Void> response = httpRequest(
                completeUrl,
                HttpMethod.PUT,
                null,
                new TypeReference<Void>() {
                }
        );
    }

    @Override
    public void deleteConnector(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s", url, connectorName);
        HttpResponse<Void> response = httpRequest(
                completeUrl,
                HttpMethod.DELETE,
                null,
                new TypeReference<Void>() {
                }
        );
    }

    @Override
    public ConnectorStateInfo getConnectorStatus(String connectorName) {
        final String completeUrl = String.format("%s/connectors/%s/status", url, connectorName);
        HttpResponse<ConnectorStateInfo> response = httpRequest(
                completeUrl,
                HttpMethod.GET,
                null,
                new TypeReference<ConnectorStateInfo>() {
                }
        );
        return response.body;
    }

    private <T> HttpResponse<T> httpRequest(String url, HttpMethod method, Object requestBodyData,
                                            TypeReference<T> responseFormat) {
        HttpClient client = new HttpClient();
        client.setFollowRedirects(false);

        try {
            client.start();
        } catch (Exception e) {
            log.error("Failed to start RestClient: ", e);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "Failed to start RestClient: " + e.getMessage(), e);
        }

        try {
            String serializedBody = requestBodyData == null ? null : JSON_SERDE.writeValueAsString(requestBodyData);
            log.trace("Sending {} with input {} to {}", method, serializedBody, url);

            Request req = client.newRequest(url);
            req.method(method);
            req.accept("application/json");
            req.agent("testing-connect");

            if (serializedBody != null) {
                req.content(new StringContentProvider(serializedBody, StandardCharsets.UTF_8), "application/json");
            }

            ContentResponse res = req.send();

            int responseCode = res.getStatus();
            log.debug("Request's response code: {}", responseCode);
            if (responseCode == HttpStatus.NO_CONTENT_204) {
                return new HttpResponse<>(responseCode, convertHttpFieldsToMap(res.getHeaders()), null);
            } else if (responseCode >= 400) {
                ErrorMessage errorMessage = JSON_SERDE.readValue(res.getContentAsString(), ErrorMessage.class);
                throw new ConnectRestException(responseCode, errorMessage.errorCode(), errorMessage.message());
            } else if (responseCode >= 200 && responseCode < 300) {
                T result = JSON_SERDE.readValue(res.getContentAsString(), responseFormat);
                return new HttpResponse<>(responseCode, convertHttpFieldsToMap(res.getHeaders()), result);
            } else {
                throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR,
                        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                        "Unexpected status code when handling forwarded request: " + responseCode);
            }
        } catch (IOException | InterruptedException | TimeoutException | ExecutionException e) {
            log.error("IO error forwarding REST request: ", e);
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR, "IO Error trying to forward REST request: " + e.getMessage(), e);
        } finally {
            try {
                client.stop();
            } catch (Exception e) {
                log.error("Failed to stop HTTP client", e);
            }
        }
    }

    /**
     * Convert response parameters from Jetty format (HttpFields)
     */
    private Map<String, String> convertHttpFieldsToMap(HttpFields httpFields) {
        Map<String, String> headers = new HashMap<>();

        if (httpFields == null || httpFields.size() == 0)
            return headers;

        for (HttpField field : httpFields) {
            headers.put(field.getName(), field.getValue());
        }

        return headers;
    }

    private class HttpResponse<T> {
        private final int status;
        private final Map<String, String> headers;
        private final T body;

        public HttpResponse(int status, Map<String, String> headers, T body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        public int status() {
            return status;
        }

        public Map<String, String> headers() {
            return headers;
        }

        public T body() {
            return body;
        }
    }
}
