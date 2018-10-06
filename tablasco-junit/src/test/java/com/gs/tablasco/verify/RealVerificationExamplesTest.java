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

package com.gs.tablasco.verify;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.HtmlTestUtil;
import com.gs.tablasco.TableTestUtils;
import com.gs.tablasco.compare.ColumnComparators;
import com.gs.tablasco.compare.DefaultComparableTableAdapter;
import com.gs.tablasco.compare.ResultTable;
import com.gs.tablasco.compare.indexmap.IndexMapTableComparator;
import com.gs.tablasco.rebase.RebaseFileWriter;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.ParsedResults;
import com.gs.tablasco.results.parser.TableDataParser;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

public class RealVerificationExamplesTest
{
    // can be set to true to obfuscate data before checkin (needs to be reverted back to false or tests will fail)
    private static final boolean OBFUSCATE = false;

    @Test
    public void BsGlReconciliationRegressionTest_usGaapTestsGscmInventoryRecTrue()
    {
        verify("BsGlReconciliationRegressionTest", "usGaapTestsGscmInventoryRecTrue", "GL Reconciliation Results", 2329); // was 3708
    }

    @Test
    public void BswCpsAnalysisRegressionTest_regrBalanceBusinessType()
    {
        verify("BswCpsAnalysisRegressionTest", "regrBalanceBusinessType", "results", 483); // was 491
    }

    @Test
    public void JournalCreationRegressionTest_usGaapPnl()
    {
        verify("JournalCreationRegressionTest", "usGaapPnl", "journals", 89789); // was 113840 and 90090
    }

    @Test
    public void RepoNetdownAllocationBswLiteTest_groupByDeskheadAndIncomeFunction()
    {
        verify("RepoNetdownAllocationBswLiteTest", "groupByDeskheadAndIncomeFunction", "Side-by-side", 36); // was 38
    }

    @Test
    public void UserQueryTestGsBankRegressionTest_gsbLoansCr()
    {
        verify("UserQueryTestGsBankRegressionTest", "gsbLoansCr", "Side-by-side", 8);
    }

    private static void verify(String className, String methodName, String tableName, int expectedBrokenCells)
    {
        File examplesDir = new File(TableTestUtils.getExpectedDirectory(), "examples");
        File actual = new File(examplesDir, className + '_' + methodName + "_ACTUAL.txt");
        File expected = new File(examplesDir, actual.getName().replace("_ACTUAL.txt", "_EXPECTED.txt"));
        ParsedResults actualResults = new TableDataParser(new FileSystemExpectedResultsLoader(), actual).parse();
        ParsedResults expectedResults = new TableDataParser(new FileSystemExpectedResultsLoader(), expected).parse();
        Map<String, ComparableTable> actualTables = actualResults.getTables(methodName);
        Map<String, ComparableTable> expectedTables = expectedResults.getTables(methodName);
        ColumnComparators columnComparators = new ColumnComparators.Builder().withTolerance(1.0d).build();
        if (OBFUSCATE)
        {
            Obfuscator obfuscator = new Obfuscator();
            adaptForObfuscation(actualTables, obfuscator);
            adaptForObfuscation(expectedTables, obfuscator);
            new RebaseFileWriter(actualResults.getMetadata(), new String[0], columnComparators, actual).writeRebasedResults(methodName, TableTestUtils.adapt(actualTables));
            new RebaseFileWriter(expectedResults.getMetadata(), new String[0], columnComparators, expected).writeRebasedResults(methodName, TableTestUtils.adapt(expectedTables));
        }
        Assert.assertFalse("Obfuscation is only to help prepare results for checkin", OBFUSCATE);
        Assert.assertTrue(!actualTables.isEmpty());
        Assert.assertEquals(actualTables.keySet(), expectedTables.keySet());

        IndexMapTableComparator comparator = new IndexMapTableComparator(columnComparators, false, IndexMapTableComparator.DEFAULT_BEST_MATCH_THRESHOLD, false, false);
        ResultTable verify = comparator.compare(actualTables.get(tableName), expectedTables.get(tableName));

        HtmlTestUtil.append(
                new File(TableTestUtils.getOutputDirectory(), RealVerificationExamplesTest.class.getSimpleName() + '_' + className + '_' + methodName + ".html").toPath(),
                methodName, tableName, verify
        );

        int failedCells = verify.getTotalCellCount() - verify.getPassedCellCount();
        Assert.assertEquals(expectedBrokenCells, failedCells);
    }

    private static void adaptForObfuscation(Map<String, ComparableTable> tables, final Obfuscator obfuscator)
    {
        for (String key : tables.keySet().toArray(new String[0]))
        {
            tables.put(key, new ObfuscatingTableAdapter(tables.get(key), obfuscator));
        }
    }

    private static class ObfuscatingTableAdapter extends DefaultComparableTableAdapter
    {
        private final Obfuscator obfuscator;

        ObfuscatingTableAdapter(ComparableTable delegate, Obfuscator obfuscator)
        {
            super(delegate);
            this.obfuscator = obfuscator;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            Object valueAt = super.getValueAt(rowIndex, columnIndex);
            return valueAt instanceof String ? this.obfuscator.obfuscate((String) valueAt) : valueAt;
        }
    }

    private static class Obfuscator
    {
        private static final char[] consonants = {'q', 'w', 'r', 't', 'y', 'p', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm'};
        private static final char[] vowels = {'a', 'e', 'i', 'o', 'u'};

        private static final Random rand = new Random(12345987345909L);

        private HashSet<String> immutables = new HashSet<>();
        private HashMap<String, String> replacements = new HashMap<>();

        private String makeWord(int length)
        {
            StringBuilder builder = new StringBuilder(length);
            for (int i = 0; i < length; i++)
            {
                builder.append((i & 1) == 0 ? consonants[rand.nextInt(consonants.length)] : vowels[rand.nextInt(vowels.length)]);
            }
            return builder.toString();
        }

        String obfuscate(String toReplace)
        {
            if (immutables.contains(toReplace))
            {
                return toReplace;
            }
            String lower = toReplace.toLowerCase();
            String existing = replacements.get(lower);
            if (existing == null)
            {
                existing = makeWord(lower.length());
                replacements.put(lower, existing);
            }
            return makeSameCapCase(toReplace, existing);
        }

        private String makeSameCapCase(String toReplace, String existing)
        {
            StringBuilder builder = new StringBuilder(toReplace.length());
            for (int i = 0; i < toReplace.length(); i++)
            {
                builder.append(Character.isUpperCase(toReplace.charAt(i)) ? Character.toUpperCase(existing.charAt(i)) : existing.charAt(i));
            }
            return builder.toString();
        }

    }
}