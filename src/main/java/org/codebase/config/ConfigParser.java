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

package org.codebase.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CatalogUtil;
import org.codebase.CatalogType;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConfigParser {

    private static final String argPrefix = "--";

    private static final String restServerBindIpKey = "rest.bind.ip";
    private static final String restServerBindIp = "0.0.0.0";
    private static final String restServerBindPortKey = "rest.bind.port";
    private static final int restServerBindPort = 13201;
    private static final String catalogTypeKey = CatalogUtil.ICEBERG_CATALOG_TYPE;

    private final String[] args;

    public ConfigParser(String[] commandlineArgs) {
        this.args = commandlineArgs;
    }

    public static Map<String, String> propertyWithPrefix(Map<String, String> property, String prefix) {
        if (StringUtils.isEmpty(prefix)) {
            return property;
        }
        return property.entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> {
                    String key = entry.getKey().substring(prefix.length());
                    String value = entry.getValue();
                    return new ConfigRecord(key, value);
                })
                .collect(Collectors.toMap(ConfigRecord::key, ConfigRecord::value));
    }

    private Map<String, String> parse() {
        Properties properties = System.getProperties();
        Map<String, String> configMap = new HashMap<>();
        properties.forEach((k, v) -> configMap.put(k.toString(), v.toString()));
        for (int i = 0; i < args.length; i++) {
            String currentArg = args[i];
            if (currentArg.startsWith(argPrefix)) {
                String key = currentArg.substring(argPrefix.length());
                String value = args[++i];
                configMap.put(key, value);
            } else {
                throw new IllegalArgumentException("Unrecognized option: " + currentArg);
            }
        }
        return configMap;
    }

    public CatalogConfig getCatalogConfig() {
        Map<String, String> allConfig = parse();
        if (allConfig.containsKey(catalogTypeKey)) {
            CatalogType catalogType = CatalogType.fromString(allConfig.get(catalogTypeKey));
            String catalogConfigPrefix = "catalog." + catalogType.getTypeName() + ".";
            return new CatalogConfig(catalogType, propertyWithPrefix(allConfig, catalogConfigPrefix));
        } else {
            throw new IllegalArgumentException("Key: " + catalogTypeKey + " not specified!");
        }
    }

    public ServerConfig getServerConfig() {
        Map<String, String> allConfig = parse();
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

    record ConfigRecord(String key, String value) {

        public static ConfigRecord of(String key, String value) {
            return new ConfigRecord(key, value);
        }

        public static ConfigRecord of(Map.Entry<String, String> entry) {
            return of(entry.getKey(), entry.getValue());
        }

    }


}
