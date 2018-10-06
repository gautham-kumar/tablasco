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

package com.gs.tablasco.results.parser;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.ParsedResults;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.IdentityHashMap;
import java.util.Map;

public class ExpectedResultsParserTest
{
    @Test
    public void testParse()
    {
        File expected = new File(TableTestUtils.getExpectedDirectory(), ExpectedResultsParserTest.class.getSimpleName() + ".txt");

        ParsedResults results = new TableDataParser(new FileSystemExpectedResultsLoader(), expected).parse();

        ComparableTable summary = results.getTable("Summary");
        Assert.assertEquals(6, summary.getColumnCount());
        Assert.assertEquals(5, summary.getRowCount());

        ComparableTable drillDown = results.getTable("DrillDown");
        Assert.assertEquals(6, drillDown.getColumnCount());
        Assert.assertEquals(1, drillDown.getRowCount());

        Assert.assertEquals(2, results.getMetadata().getData().size());
        Assert.assertEquals(Tuples.pair("Recorded At", "2013-06-26 12:00:00"), results.getMetadata().getData().get(0));
        Assert.assertEquals(Tuples.pair("App Server URL", "http://test"), results.getMetadata().getData().get(1));
    }

    @Test
    public void testCache()
    {
        File expected = new File(TableTestUtils.getExpectedDirectory(), ExpectedResultsParserTest.class.getSimpleName() + ".txt");
        Map<ParsedResults, String> results = new IdentityHashMap<>();
        for (int i = 0; i < 10; i++)
        {
            results.put(ExpectedResultsCache.getExpectedResults(new FileSystemExpectedResultsLoader(), expected), "");
        }
        Assert.assertTrue("cache was hit at least once", results.size() < 10);
    }

    @Test
    public void testMissingExpectedResultsFileResultsInClearErrorMessage()
    {
        String missingFileName = "missing-expected-results.txt";
        try
        {
            new TableDataParser(new FileSystemExpectedResultsLoader(), new File(missingFileName)).parse();
            Assert.fail("Should have failed looking for non-existent file");
        }
        catch (IllegalStateException expected)
        {
            Assert.assertTrue(expected.getMessage().contains(missingFileName));
        }
    }
}
