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

package com.gs.tablasco.adapters;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.TableVerifier;
import org.junit.Rule;
import org.junit.Test;

public class TableAdaptersTest
{
    @Rule
    public final TableVerifier verifier = new TableVerifier()
            .withHideMatchedRows(true)
            .withHideMatchedTables(true);

    @Test
    public void acceptAllRows()
    {
        this.verify(
                TableTestUtils.createTable("name", 1, "C", 1, 2, 3, 4, 5),
                TableAdapters.withRows(TableTestUtils.createTable("name", 1, "C", 1, 2, 3, 4, 5), (i, comparableTable) -> true));
    }


    @Test
    public void acceptNoRows()
    {
        this.verify(
                TableTestUtils.createTable("name", 1, "C"),
                TableAdapters.withRows(TableTestUtils.createTable("name", 1, "C", 1, 2, 3, 4, 5), (i, comparableTable) -> false));
    }

    @Test
    public void acceptSomeRows()
    {
        this.verify(
                TableTestUtils.createTable("name", 1, "C", 2, 4),
                TableAdapters.withRows(TableTestUtils.createTable("name", 1, "C", 1, 2, 3, 4, 5), (i, comparableTable) -> (Integer) comparableTable.getValueAt(i, 0) % 2 == 0));
    }

    @Test
    public void acceptAllColumns()
    {
        this.verify(
                TableTestUtils.createTable("name", 5, "C1", "C2", "C3", "C4", "C5"),
                TableAdapters.withColumns(TableTestUtils.createTable("name", 5, "C1", "C2", "C3", "C4", "C5"), name -> true));
    }

    @Test
    public void acceptSomeColumns()
    {
        this.verify(
                TableTestUtils.createTable("name", 3, "C1", "C3", "C5"),
                TableAdapters.withColumns(TableTestUtils.createTable("name", 5, "C1", "C2", "C3", "C4", "C5"), name -> name.matches("C[135]")));
    }

    @Test
    public void composition1()
    {
        ComparableTable table = TableTestUtils.createTable("name", 2, "C1", "C2", 1, 2, 3, 4);
        ComparableTable rowFilter = TableAdapters.withRows(TableAdapters.withColumns(table, name -> name.equals("C2")), (i, comparableTable) -> i > 0);
        this.verify(TableTestUtils.createTable("name", 1, "C2", 4), rowFilter);
    }

    @Test
    public void composition2()
    {
        ComparableTable table = TableTestUtils.createTable("name", 2, "C1", "C2", 1, 2, 3, 4);
        ComparableTable columnFilter = TableAdapters.withColumns(TableAdapters.withRows(table, (i, comparableTable) -> i > 0), name -> name.equals("C2"));
        this.verify(TableTestUtils.createTable("name", 1, "C2", 4), columnFilter);
    }

    private void verify(ComparableTable expected, ComparableTable adaptedActual)
    {
        this.verifier.compare(adaptedActual, expected);
    }
}
