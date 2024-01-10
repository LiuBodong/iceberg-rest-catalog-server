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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum CatalogType {
    HADOOP(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP),
    HIVE_METASTORE(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);

    private static final Map<String, CatalogType> CATALOG_TYPE_MAP = new HashMap<>();

    static {
        for (CatalogType value : CatalogType.values()) {
            CATALOG_TYPE_MAP.put(value.getTypeName(), value);
        }
    }

    private final String typeName;

    CatalogType(String typeName) {
        this.typeName = typeName;
    }

    public static CatalogType fromString(String typeName) {
        if (CATALOG_TYPE_MAP.containsKey(typeName)) {
            return CATALOG_TYPE_MAP.get(typeName);
        } else {
            String availableTypes = Arrays.stream(CatalogType.values())
                    .map(CatalogType::getTypeName)
                    .collect(Collectors.joining(","));
            throw new IllegalArgumentException("No such catalog type: " + typeName +
                    ", available types are: [" + availableTypes + "]");
        }
    }

    public String getTypeName() {
        return typeName;
    }

}
