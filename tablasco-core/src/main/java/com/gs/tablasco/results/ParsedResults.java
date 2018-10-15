/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.gs.tablasco.results;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.compare.Metadata;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParsedResults
{
    private final Map<String, Map<String, ComparableTable>> tablesByName = new LinkedHashMap<>();
    private final Metadata metadata = Metadata.newEmpty();

    public ComparableTable getTable(String sectionName)
    {
        return this.getTable(sectionName, null);
    }

    public Map<String, ComparableTable> getTables(String sectionName)
    {
        Map<String, ComparableTable> tables = this.tablesByName.get(sectionName);
        return tables == null ? Collections.emptyMap() : tables;
    }

    public ComparableTable getTable(String sectionName, String tableName)
    {
        return this.getTables(sectionName).get(translateTableName(tableName));
    }

    public void addTable(String sectionName, String tableName, ComparableTable table)
    {
        Map<String, ComparableTable> tables = this.tablesByName.get(sectionName);
        if (tables == null)
        {
            tables = new LinkedHashMap<>();
            this.tablesByName.put(sectionName, tables);
        }
        String key = translateTableName(tableName);
        if (tables.containsKey(key))
        {
            throw new IllegalStateException("Duplicate lhs table detected: " + sectionName + '/' + tableName);
        }
        tables.put(key, table);
    }

    private static String translateTableName(String tableName)
    {
        return tableName == null ? "" : tableName;
    }

    public void addMetadata(String key, String value)
    {
        this.metadata.add(key, value);
    }

    public Metadata getMetadata()
    {
        return this.metadata;
    }
}
