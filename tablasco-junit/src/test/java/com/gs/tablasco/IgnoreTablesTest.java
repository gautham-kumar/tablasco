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

import org.eclipse.collections.impl.factory.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class IgnoreTablesTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test
    public void ignoreTables() throws IOException
    {
        VerifiableTable tableA = new DefaultVerifiableTableAdapter(TableTestUtils.createTable("name", 1, "Col 1", "A"));
        VerifiableTable tableX = new DefaultVerifiableTableAdapter(TableTestUtils.createTable("name", 1, "Col 1", "X"));

        this.tableVerifier.withIgnoreTables("table1", "table3").verify(
                Maps.fixedSize.of("table1", tableA, "table2", tableA, "table3", tableX),
                Maps.fixedSize.of("table1", tableX, "table2", tableA, "table3", tableA));

        Assert.assertEquals(
                "<body>\n" +
                        "<div class=\"metadata\"/>\n" +
                        "<h1>ignoreTables</h1>\n" +
                        "<div id=\"ignoreTables.table2\">\n" +
                        "<h2>table2</h2>\n" +
                        "<table border=\"1\" cellspacing=\"0\">\n" +
                        "<tr>\n" +
                        "<th class=\"pass\">Col 1</th>\n" +
                        "</tr>\n" +
                        "<tr>\n" +
                        "<td class=\"pass\">A</td>\n" +
                        "</tr>\n" +
                        "</table>\n" +
                        "</div>\n" +
                        "</body>", TableTestUtils.getHtml(this.tableVerifier, "body"));
    }
}