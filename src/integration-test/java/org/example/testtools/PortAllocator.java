package org.example.testtools;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PortAllocator {
    private static final Logger LOG = LoggerFactory.getLogger(PortAllocator.class);
    private static final int AUTO_ALLOCATE = 0;

    private static final Set<Integer> allocatedPorts = new HashSet<>();

    public static synchronized List<Integer> findAndAllocatePorts(int nrOfPorts) {
        final List<Integer> localPorts = new ArrayList<>(nrOfPorts);
        final List<ServerSocket> serverSockets = new ArrayList<>(nrOfPorts);
        try {
            while (localPorts.size() < nrOfPorts) {
                ServerSocket socket = new ServerSocket(AUTO_ALLOCATE);
                serverSockets.add(socket);
                if (!allocatedPorts.contains(socket.getLocalPort())) {
                    localPorts.add(socket.getLocalPort());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not allocate free ports", e);
        } finally {
            serverSockets.forEach(socket -> {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.info("Could not close socket", e);
                }
            });
        }

        allocatedPorts.addAll(localPorts);
        return localPorts;
    }

    public static synchronized void deallocatePorts(List<Integer> ports) {
        allocatedPorts.removeAll(ports);
    }
}
