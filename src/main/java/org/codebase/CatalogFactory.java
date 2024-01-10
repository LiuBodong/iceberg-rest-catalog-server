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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.codebase.config.CatalogConfig;
import org.codebase.config.ConfigParser;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class CatalogFactory {

    public static Catalog createCatalog(CatalogConfig catalogConfig) {
        CatalogType catalogType = catalogConfig.catalogType();
        Map<String, String> catalogProperties = catalogConfig.catalogConfig();
        switch (catalogType) {
            case HADOOP -> {
                String hadoopConfDir = null;
                Configuration configuration = new Configuration(true);
                if (catalogProperties.containsKey("hadoopConfDir")) {
                    hadoopConfDir = catalogProperties.get("hadoopConfDir");
                } else {
                    String envHadoopConfDir = System.getenv("HADOOP_CONF_DIR");
                    if (StringUtils.isNotEmpty(envHadoopConfDir)) {
                        hadoopConfDir = envHadoopConfDir;
                    }
                }
                if (StringUtils.isEmpty(hadoopConfDir)) {
                    throw new IllegalArgumentException("Please specify option catalog.hadoop.hadoopConfDir or System Env HADOOP_CONF_DIR");
                }
                final String[] files = {"core-site.xml", "hdfs-site.xml"};
                for (String file : files) {
                    Path path = Paths.get(hadoopConfDir, file);
                    if (Files.exists(path)) {
                        try {
                            InputStream inputStream = Files.newInputStream(path);
                            configuration.addResource(inputStream);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                }
                Map<String, String> hadoopProperties = ConfigParser.propertyWithPrefix(catalogProperties, "hadoop.");
                hadoopProperties.forEach(configuration::set);
                if (catalogProperties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
                    return new HadoopCatalog(configuration, catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION));
                } else {
                    throw new IllegalArgumentException("Please set catalog." + catalogType.getTypeName() + "." + CatalogProperties.WAREHOUSE_LOCATION);
                }
            }
            default -> {
                throw new IllegalArgumentException("Catalog type: " + catalogType.getTypeName() + " current not support!");
            }
        }
    }

}
