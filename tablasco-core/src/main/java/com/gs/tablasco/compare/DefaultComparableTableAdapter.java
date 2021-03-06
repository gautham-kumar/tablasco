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

/**
 * A default <tt>ComparableTable</tt> adapter that delegates all calls to an underlying delegate table. Extend this
 * class if you only need to modify behaviour of some methods of the underlying table.
 */
public abstract class DefaultComparableTableAdapter implements ComparableTable
{
    private final String tableName;
    private final ComparableTable delegate;

    /**
     * Creates a new <tt>DefaultComparableTableAdapter</tt> with an underlying table to which calls should be delegated.
     *
     * @param delegate underlying table to which calls should be delegated
     */
    protected DefaultComparableTableAdapter(ComparableTable delegate)
    {
        this(delegate.getTableName(), delegate);
    }

    public DefaultComparableTableAdapter(String tableName, ComparableTable delegate)
    {
        this.tableName = tableName;
        this.delegate = delegate;
    }

    @Override
    public String getTableName()
    {
        return this.tableName;
    }

    @Override
    public int getRowCount()
    {
        return this.delegate.getRowCount();
    }

    @Override
    public int getColumnCount()
    {
        return this.delegate.getColumnCount();
    }

    @Override
    public String getColumnName(int columnIndex)
    {
        return this.delegate.getColumnName(columnIndex);
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex)
    {
        return this.delegate.getValueAt(rowIndex, columnIndex);
    }
}
