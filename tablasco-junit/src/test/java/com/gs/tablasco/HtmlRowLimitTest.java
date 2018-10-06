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

package com.gs.tablasco;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class HtmlRowLimitTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy()
            .withHtmlRowLimit(3);

    @Test
    public void tablesMatch() throws IOException
    {
        ComparableTable table = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        this.tableVerifier.compare(table, table);
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                        "<tr>\n" +
                        "<th class=\"pass\">Col 1</th>\n" +
                        "<th class=\"pass\">Col 2</th>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">A1</td>\n" +
                        "<td class=\"pass\">A2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">B1</td>\n" +
                        "<td class=\"pass\">B2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">C1</td>\n" +
                        "<td class=\"pass\">C2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass multi\" colspan=\"2\">2 more rows...</td>\n" +
                        "</tr>\n" +
                        "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void tablesDoNotMatch() throws IOException
    {
        final ComparableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final ComparableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.compare(table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                        "<tr>\n" +
                        "<th class=\"pass\">Col 1</th>\n" +
                        "<th class=\"pass\">Col 2</th>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">A1</td>\n" +
                        "<td class=\"pass\">A2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">B1</td>\n" +
                        "<td class=\"pass\">B2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">C1</td>\n" +
                        "<td class=\"pass\">C2</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"fail multi\" colspan=\"2\">2 more rows...</td>\n" +
                        "</tr>\n" +
                        "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void hideMatchedRows() throws IOException
    {
        final ComparableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2");
        final ComparableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "AX", "B1", "B2", "C1", "C2", "D1", "DX", "E1", "E2");
        TableTestUtils.assertAssertionError(() -> tableVerifier.withHideMatchedRows(true).compare(table1, table2));
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                        "<tr>\n" +
                        "<th class=\"pass\">Col 1</th>\n" +
                        "<th class=\"pass\">Col 2</th>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">A1</td>\n" +
                        "<td class=\"fail\">A2<p>Expected</p>\n" +
                        "<hr/>AX<p>Actual</p>\n" +
                        "</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">D1</td>\n" +
                        "<td class=\"fail\">D2<p>Expected</p>\n" +
                        "<hr/>DX<p>Actual</p>\n" +
                        "</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"fail multi\" colspan=\"2\">1 more row...</td>\n" +
                        "</tr>\n" +
                        "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }

    @Test
    public void hideMatchedRows2() throws IOException
    {
        final ComparableTable table1 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2");
        final ComparableTable table2 = TableTestUtils.createTable(2, "Col 1", "Col 2", "A1", "A2", "B1", "B2", "C1", "CX", "D1", "DX");
        TableTestUtils.assertAssertionError(() ->
        {
            tableVerifier.withHtmlRowLimit(1).withHideMatchedRows(true).compare(table1, table2);
            ;
        });
        Assert.assertEquals(
                "<table border=\"1\" cellspacing=\"0\">\n" +
                        "<tr>\n" +
                        "<th class=\"pass\">Col 1</th>\n" +
                        "<th class=\"pass\">Col 2</th>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass multi\" colspan=\"2\">2 matched rows...</td>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"fail multi\" colspan=\"2\">2 more rows...</td>\n" +
                        "</tr>\n" +
                        "</table>", TableTestUtils.getHtml(this.tableVerifier, "table"));
    }
}