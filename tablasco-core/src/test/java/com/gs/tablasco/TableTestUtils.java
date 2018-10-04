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

import com.gs.tablasco.compare.ListComparableTable;
import com.gs.tablasco.results.parser.TableDataParser;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;

public class TableTestUtils
{
    static final String TABLE_NAME = "peopleTable";

    static final ComparableTable TEST_DATA_1 = new ListComparableTable(
            TABLE_NAME,
            Arrays.<Object>asList("First", "Last", "Age"),
            Arrays.asList(
                    Arrays.asList("Barry", "White", 21.3),
                    Arrays.asList("Oscar", "White", 7.6)
            ));

    static final ComparableTable TEST_DATA_2 = new ListComparableTable(
            TABLE_NAME,
            Arrays.<Object>asList("First", "Last", "Age"),
            Collections.singletonList(
                    Arrays.asList("Elliot", "White", 3.8)));

    static final ComparableTable TEST_DATA_3 = new ListComparableTable(
            TABLE_NAME,
            Arrays.<Object>asList("Name", "Age", "Weight", "Height"),
            Collections.singletonList(
                    Arrays.asList("Elliot", 1.1, 1.02, 1.5)));

    public static class TestInputReader extends TestWatcher
    {
        private final TableDataParser tableDataParser;

        private Description description;

        TestInputReader(File testInputFile)
        {
            this.tableDataParser  = new TableDataParser(FileInputStream::new, testInputFile);
        }

        @Override
        protected void starting(Description description)
        {
            this.description = description;
        }

        ComparableTable getLhsTableForTest(String tableName)
        {
            return tableDataParser.parse().getTable(this.description.getMethodName(), tableName);
        }
    }
}
