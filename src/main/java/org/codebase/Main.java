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

package org.codebase;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.codebase.config.CatalogConfig;
import org.codebase.config.ConfigParser;
import org.codebase.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String restServerBindIpKey = "rest.bind.ip";
    private static final String restServerBindIp = "0.0.0.0";
    private static final String restServerBindPortKey = "rest.bind.port";
    private static final int restServerBindPort = 13201;
    private static final String catalogTypeKey = CatalogUtil.ICEBERG_CATALOG_TYPE;

    public static void main(String[] args) throws Exception {
        ConfigParser configParser = new ConfigParser(args);
        Map<String, String> allConfig = configParser.parse();
        ServerConfig serverConfig = getServerConfig(allConfig);
        LOGGER.info("Server config: {}", serverConfig);
        CatalogConfig catalogConfig = getCatalogConfig(allConfig);
        LOGGER.info("Catalog config: {}", catalogConfig);
        Catalog catalog = CatalogFactory.createCatalog(catalogConfig);
        RestCatalogServer restCatalogServer = new RestCatalogServer(catalog, serverConfig);
        restCatalogServer.start();
    }

    public static CatalogConfig getCatalogConfig(Map<String, String> allConfig) {
        if (allConfig.containsKey(catalogTypeKey)) {
            CatalogType catalogType = CatalogType.fromString(allConfig.get(catalogTypeKey));
            String catalogConfigPrefix = "catalog." + catalogType.getTypeName() + ".";
            return new CatalogConfig(catalogType, ConfigParser.propertyWithPrefix(allConfig, catalogConfigPrefix));
        } else {
            throw new IllegalArgumentException("Key: " + catalogTypeKey + " not specified!");
        }
    }

    public static ServerConfig getServerConfig(Map<String, String> allConfig) {
        String host = restServerBindIp;
        int port = restServerBindPort;
        if (allConfig.containsKey(restServerBindIpKey)) {
            host = allConfig.get(restServerBindIpKey);
        }
        if (allConfig.containsKey(restServerBindPortKey)) {
            port = Integer.parseInt(allConfig.get(restServerBindPortKey));
        }
        return new ServerConfig(host, port);
    }
}