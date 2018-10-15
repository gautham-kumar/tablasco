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

package com.gs.tablasco.compare;

import com.gs.tablasco.ComparableTable;
import org.eclipse.collections.impl.utility.Iterate;

import java.util.List;

public class ListComparableTable implements ComparableTable
{
    private final String tableName;
    private final List<?> headers;
    private final List<List<Object>> data;

    public ListComparableTable(String tableName, List<List<Object>> headersAndData)
    {
        this(tableName, Iterate.getFirst(headersAndData), headersAndData.subList(1, headersAndData.size()));
    }

    public ListComparableTable(String tableName, List<?> headers, List<List<Object>> data)
    {
        this.tableName = tableName;
        this.headers = headers;
        this.data = data;
    }

    @Override
    public String getTableName()
    {
        return this.tableName;
    }

    @Override
    public int getRowCount()
    {
        return this.data.size();
    }

    @Override
    public int getColumnCount()
    {
        return this.headers.size();
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return String.valueOf(this.headers.get(columnIndex));
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return this.data.get(rowIndex).get(columnIndex);
    }
}
