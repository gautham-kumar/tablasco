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

import com.gs.tablasco.compare.Metadata;
import com.gs.tablasco.files.*;
import com.gs.tablasco.lifecycle.DefaultExceptionHandler;
import com.gs.tablasco.lifecycle.DefaultLifecycleEventHandler;
import com.gs.tablasco.lifecycle.ExceptionHandler;
import com.gs.tablasco.lifecycle.LifecycleEventHandler;
import com.gs.tablasco.rebase.Rebaser;
import com.gs.tablasco.results.FileSystemExpectedResultsLoader;
import com.gs.tablasco.results.ParsedResults;
import com.gs.tablasco.results.TableDataLoader;
import com.gs.tablasco.results.parser.ExpectedResultsCache;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.list.fixed.ArrayAdapter;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.Iterate;
import org.eclipse.collections.impl.utility.ListIterate;
import org.eclipse.collections.impl.utility.MapIterate;
import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A JUnit <tt>Rule</tt> that can be included in JUnit 4 tests and used for verifying tabular data represented as
 * instances of <tt>VerifiableTable</tt>. <tt>TableVerifier</tt> can compare actual and expected tables provided by the
 * test or, more commonly, compares actual tables with expected results stored in the filesystem (the baseline). When
 * expected results are stored in the filesystem there are two modes of operation: rebase mode and the default verify
 * mode.
 * <p>
 * In rebase mode actual results provided by the test are saved in the configured expected results directory. No
 * verification is performed and the test always fails to ensure that rebase is not enabled accidentally. Rebase mode
 * can be enabled by setting the system property <ii>rebase</ii> to <ii>true</ii> or by calling <ii>.withRebase()</ii>.
 * <p>
 * In the default verify mode expected results are read from the filesystem and compared with the actual results
 * provided by the test. If the actual and expected tables match the test passes, otherwise the test fails. The
 * verification results are published as an HTML file in the configured output directory.
 * <p>
 * A number of configuration options are available to influence the behaviour of <tt>TableVerifier</tt>. A fluent
 * interface allows configuration options to be combined in a flexible manner.
 * <p>
 * <tt>TableVerifier</tt> cannot be subclassed. If custom setup and teardown logic is required, please
 * use a junit <tt>RuleChain</tt>
 * <p>
 */
public final class TableVerifier extends TableComparator<TableVerifier> implements TestRule
{
    private static final ExecutorService EXPECTED_RESULTS_LOADER_EXECUTOR = Executors.newSingleThreadExecutor(runnable ->
    {
        Thread thread = new Thread(runnable);
        thread.setName("Expected Results Loader");
        thread.setDaemon(true);
        return thread;
    });

    private File fixedExpectedDir;
    private File fixedOutputDir;
    private boolean isRebasing = Rebaser.inRebaseMode();
    private Description description;
    private FilenameStrategy fileStrategy = new FilePerMethodStrategy();
    private DirectoryStrategy directoryStrategy = new FixedDirectoryStrategy();
    private boolean createActualResults = true;
    private boolean createActualResultsOnFailure = false;
    private String[] baselineHeaders = null;
    private final Metadata metadata = Metadata.newWithRecordedAt();
    private Map<String, VerifiableTable> expectedTables;
    private Metadata expectedMetadata;
    private Set<String> tablesToAlwaysShowMatchedRowsFor = UnifiedSet.newSet();
    private Set<String> tablesNotToAdapt = UnifiedSet.newSet();
    private Predicate<String> tableFilter = s -> true;
    private TableDataLoader expectedResultsLoader = new FileSystemExpectedResultsLoader();
    private Future<ParsedResults> expectedResultsFuture;
    private LifecycleEventHandler lifecycleEventHandler = new DefaultLifecycleEventHandler();
    private ExceptionHandler exceptionHandler = new DefaultExceptionHandler();

    private final TestWatcherBridge bridge = new TestWatcherBridge(this);

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed expected results directory.
     *
     * @param expectedDirPath path to the expected results directory
     * @return this
     */
    public final TableVerifier withExpectedDir(String expectedDirPath)
    {
        return this.withExpectedDir(new File(expectedDirPath));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed expected results directory.
     *
     * @param expectedDir expected results directory
     * @return this
     */
    public final TableVerifier withExpectedDir(File expectedDir)
    {
        this.fixedExpectedDir = expectedDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(expectedDir, this.fixedOutputDir));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed verification output directory.
     *
     * @param outputDirPath path to the verification output directory
     * @return this
     */
    public final TableVerifier withOutputDir(String outputDirPath)
    {
        return this.withOutputDir(new File(outputDirPath));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a fixed verification output directory.
     *
     * @param outputDir verification output directory
     * @return this
     */
    public final TableVerifier withOutputDir(File outputDir)
    {
        this.fixedOutputDir = outputDir;
        return this.withDirectoryStrategy(new FixedDirectoryStrategy(this.fixedExpectedDir, outputDir));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a Maven style directory strategy.
     *
     * @return this
     */
    public final TableVerifier withMavenDirectoryStrategy()
    {
        return this.withDirectoryStrategy(new MavenStyleDirectoryStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a Maven style directory strategy.
     *
     * @param expectedSubDir - the folder in src/main/resources where expected files are found
     * @param outputSubDir   - the folder in target where actual files are written
     * @return this
     */
    public final TableVerifier withMavenDirectoryStrategy(String expectedSubDir, String outputSubDir)
    {
        final MavenStyleDirectoryStrategy directoryStrategy =
                new MavenStyleDirectoryStrategy()
                        .withExpectedSubDir(expectedSubDir)
                        .withOutputSubDir(outputSubDir);
        return this.withDirectoryStrategy(directoryStrategy);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with rebase mode enabled.
     *
     * @return this
     */
    public final TableVerifier withRebase()
    {
        this.isRebasing = true;
        return self();
    }

    /**
     * @return whether rebasing is enabled or not
     */
    public final boolean isRebasing()
    {
        return this.isRebasing;
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use the same expected results and verification
     * output file for each test method.
     *
     * @return this
     */
    public final TableVerifier withFilePerMethod()
    {
        return this.withFileStrategy(new FilePerMethodStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use a different expected results and
     * verification output file for each test method.
     *
     * @return this
     * @deprecated Rebase does not work correctly with this strategy which will be removed eventually. Please use the
     * default FilePerMethod instead.
     */
    @Deprecated
    public final TableVerifier withFilePerClass()
    {
        return this.withFileStrategy(new FilePerClassStrategy());
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom expected results and verification
     * output filename strategy
     *
     * @param filenameStrategy the filename stragety
     * @return this
     */
    public final TableVerifier withFileStrategy(FilenameStrategy filenameStrategy)
    {
        this.fileStrategy = filenameStrategy;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom expected results and verification
     * output directory strategy
     *
     * @param directoryStrategy the directory strategy
     * @return this
     */
    public final TableVerifier withDirectoryStrategy(DirectoryStrategy directoryStrategy)
    {
        this.directoryStrategy = directoryStrategy;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with row order verification disabled. If this is
     * disabled a test will pass if the cells match but row order is different between actual and expected results.
     *
     * @param verifyRowOrder whether to verify row order or not
     * @return this
     */
    public final TableVerifier withVerifyRowOrder(boolean verifyRowOrder)
    {
        return this.withCompareRowOrder(verifyRowOrder);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a baseline metadata name and value. This
     * metadata will be included in the baseline expected results file.
     *
     * @param name  metadata name
     * @param value metadata value
     * @return this
     */
    public final TableVerifier withMetadata(String name, String value)
    {
        this.metadata.add(name, value);
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to always show matched rows for the specified
     * tables. This only makes sense when withHideMatchedRows is true.
     *
     * @param tableNames varargs of table names to always show matched rows for
     * @return this
     */
    public final TableVerifier withAlwaysShowMatchedRowsFor(String... tableNames)
    {
        this.tablesToAlwaysShowMatchedRowsFor.addAll(ArrayAdapter.adapt(tableNames));
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to use the original unmodified results for the specified
     * tables. This only makes sense when withActualAdapter or withExpectedAdapter is enabled and means that the adapter
     * is not applied to the specified tables.
     *
     * @param tableNames varargs of table names for which original unmodified results should be displayed
     * @return this
     */
    public final TableVerifier withTablesNotToAdapt(String... tableNames)
    {
        this.tablesNotToAdapt.addAll(ArrayAdapter.adapt(tableNames));
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to create a text file of the actual results in the
     * verification output directory. This can be useful for analysis and manual rebasing.
     *
     * @return this
     */
    public final TableVerifier withCreateActualResults(boolean createActualResults)
    {
        this.createActualResults = createActualResults;
        return self();
    }

    /**
     * Limits the creation of the actual results for only tests that have failed verification.
     *
     * @return this
     */
    public final TableVerifier withCreateActualResultsOnFailure(boolean createActualResultsOnFailure)
    {
        this.createActualResultsOnFailure = createActualResultsOnFailure;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a function for adapting actual results. Each
     * table in the actual results will be adapted using the specified function before being verified or rebased.
     *
     * @param actualAdapter function for adapting tables
     * @return this
     */
    public final TableVerifier withActualAdapter(Function<VerifiableTable, VerifiableTable> actualAdapter)
    {
        return this.withRhsAdapter(comparableTable -> actualAdapter.valueOf((VerifiableTable) comparableTable));
    }

    /**
     * Returns the actual table adapter
     *
     * @return - the actual table adapter
     */
    public final Function<VerifiableTable, VerifiableTable> getActualAdapter()
    {
        return verifiableTable -> (VerifiableTable) this.getRhsAdapter().valueOf(verifiableTable);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a function for adapting expected results.
     * Each table in the expected results will be adapted using the specified function before being verified.
     *
     * @param expectedAdapter function for adapting tables
     * @return this
     */
    public final TableVerifier withExpectedAdapter(Function<VerifiableTable, VerifiableTable> expectedAdapter)
    {
        return this.withLhsAdapter(comparableTable -> expectedAdapter.valueOf((VerifiableTable) comparableTable));
    }

    /**
     * Returns the expected table adapter
     *
     * @return - the expected table adapter
     */
    public final Function<VerifiableTable, VerifiableTable> getExpectedAdapter()
    {
        return verifiableTable -> (VerifiableTable) this.getLhsAdapter().valueOf(verifiableTable);
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to ignore tables from both the actual and
     * expected results.
     *
     * @param tableNames the names of tables to ignore
     * @return this
     */
    public final TableVerifier withIgnoreTables(String... tableNames)
    {
        Set<String> tableNameSet = UnifiedSet.newSetWith(tableNames);
        return this.withTableFilter(s -> !tableNameSet.contains(s));
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to filter tables from both the actual and
     * expected results.
     *
     * @param tableFilter the table filter to apply
     * @return this
     */
    public final TableVerifier withTableFilter(Predicate<String> tableFilter)
    {
        this.tableFilter = tableFilter;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured to exclude SVN headers in expected results files
     *
     * @return this
     */
    public final TableVerifier withBaselineHeaders(String... headers)
    {
        this.baselineHeaders = headers;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom <tt>ExpectedResultsLoader</tt>
     * instance
     *
     * @param expectedResultsLoader the <tt>ExpectedResultsLoader</tt> instance
     * @return this
     */
    public final TableVerifier withExpectedResultsLoader(TableDataLoader expectedResultsLoader)
    {
        this.expectedResultsLoader = expectedResultsLoader;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom LifecycleEventHandler instance.
     *
     * @return this
     */
    public final TableVerifier withLifecycleEventHandler(LifecycleEventHandler lifecycleEventHandler)
    {
        this.lifecycleEventHandler = lifecycleEventHandler;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableVerifier</tt> configured with a custom ExceptionHandler instance.
     *
     * @return this
     */
    public final TableVerifier withExceptionHandler(ExceptionHandler exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
        return self();
    }

    public final void starting(Description description)
    {
        this.description = description;
        if (!this.isRebasing)
        {
            this.expectedResultsFuture = EXPECTED_RESULTS_LOADER_EXECUTOR.submit(() -> ExpectedResultsCache.getExpectedResults(expectedResultsLoader, getExpectedFile()));
        }
        this.lifecycleEventHandler.onStarted(description);
    }

    public final void succeeded(Description description)
    {
        try
        {
            if (MapIterate.notEmpty(this.expectedTables))
            {
                this.verifyTables(Iterate.collect(this.expectedTables.values(), table -> Tuples.pair(table, null), FastList.newList()), this.expectedMetadata);
            }
        }
        catch (AssertionError assertionError)
        {
            this.failed(assertionError, description);
            throw assertionError;
        }
        this.lifecycleEventHandler.onSucceeded(description);
        if (this.isRebasing)
        {
            Assert.fail("REBASE SUCCESSFUL - failing test in case rebase flag is set by mistake");
        }
    }

    public final void failed(Throwable e, Description description)
    {
        if (!AssertionError.class.isInstance(e))
        {
            this.exceptionHandler.onException(this.getOutputFile(), e);
        }
        this.lifecycleEventHandler.onFailed(e, description);
    }

    public final void skipped(AssumptionViolatedException e, Description description)
    {
        this.lifecycleEventHandler.onSkipped(description);
    }

    public final void finished(Description description)
    {
        this.lifecycleEventHandler.onFinished(description);
    }

    public final File getExpectedFile()
    {
        File dir = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getExpectedFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    public final File getOutputFile()
    {
        File dir = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getOutputFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    public final File getActualFile()
    {
        File dir = this.directoryStrategy.getActualDirectory(this.description.getTestClass());
        String filename = this.fileStrategy.getActualFilename(this.description.getTestClass(), this.description.getMethodName());
        return new File(dir, filename);
    }

    /**
     * Verifies a map of table names to actual tables.
     *
     * @param actualTables - actual tables by name
     */
    public final void verify(Map<String, VerifiableTable> actualTables)
    {
        this.verify(asList(actualTables));
    }

    /**
     * Verifies an actual table by name.
     *
     * @param actualTable the actual table
     */
    public final void verify(String tableName, VerifiableTable actualTable)
    {
        this.verify(Lists.fixedSize.of(new DefaultVerifiableTableAdapter(tableName, actualTable)));
    }

    /**
     * Verifies an actual table.
     *
     * @param actualTable the actual table
     */
    public final void verify(VerifiableTable actualTable)
    {
        this.verify(Lists.fixedSize.of(actualTable));
    }

    /**
     * Verifies a list of actual tables.
     *
     * @param actualTables - actual tables
     */
    public final void verify(List<VerifiableTable> actualTables)
    {
        this.runPreVerifyChecks();
        this.makeSureDirectoriesAreNotSame();

        if (this.isRebasing)
        {
            this.newRebaser().rebase(this.description.getMethodName(), filterTables(actualTables), this.getExpectedFile());
        }
        else
        {
            if (this.expectedTables == null)
            {
                Optional<ParsedResults> parsedResults = getExpectedResults();
                if (!parsedResults.isPresent())
                {
                    throw new IllegalStateException("Expected results SHOULD be available in default mode");
                }
                ParsedResults expectedResults = parsedResults.get();
                Map<String, ComparableTable> tables = expectedResults.getTables(this.description.getMethodName());
                this.expectedTables = MapIterate.collect(tables, Functions.getPassThru(), DefaultVerifiableTableAdapter::new, UnifiedMap.newMap());
                this.expectedMetadata = expectedResults.getMetadata();
            }
            this.verifyTables(ListIterate.collect(actualTables, actual -> Tuples.pair(this.expectedTables.remove(actual.getTableName()), actual)), this.expectedMetadata);
        }
    }

    public final Optional<ParsedResults> getExpectedResults()
    {
        try
        {
            return this.isRebasing ? Optional.empty() : Optional.of(this.expectedResultsFuture.get());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private Rebaser newRebaser()
    {
        return new Rebaser(this.getColumnComparators(), this.metadata, this.baselineHeaders);
    }

    /**
     * Verifies a map of table names to expected tables with a map of table names to actual tables.
     *
     * @param expectedTables - expected tables by name
     * @param actualTables   - actual tables by name
     */
    public final void verify(Map<String, VerifiableTable> expectedTables, Map<String, VerifiableTable> actualTables)
    {
        this.verify(asList(expectedTables), asList(actualTables));
    }

    /**
     * Verifies a list of expected tables with a list of actual tables.
     *
     * @param expectedTables - expected tables
     * @param actualTables   - actual tables
     */
    public final void verify(List<VerifiableTable> expectedTables, List<VerifiableTable> actualTables)
    {
        this.runPreVerifyChecks();
        if (!this.isRebasing)
        {
            Map<String, VerifiableTable> actualMap = Iterate.toMap(actualTables, VerifiableTable::getTableName, Functions.getPassThru());

            List<Pair<VerifiableTable, VerifiableTable>> expectedAndActualTables = FastList.newList();

            for (VerifiableTable expectedTable : expectedTables)
            {
                expectedAndActualTables.add(Tuples.pair(expectedTable, actualMap.remove(expectedTable.getTableName())));
            }

            for (VerifiableTable table : actualMap.values())
            {
                expectedAndActualTables.add(Tuples.pair(null, table));
            }

            this.verifyTables(expectedAndActualTables, null);
        }
    }

    // to be called from tests
    private void verifyTables(List<Pair<VerifiableTable, VerifiableTable>> expectedAndActualTables, Metadata metadata)
    {
        ComparisonResult comparisonResult = ComparisonResult.newEmpty(this.newHtmlFormatter());

        for (Pair<VerifiableTable, VerifiableTable> expectedAndActualTable : expectedAndActualTables)
        {
            VerifiableTable expected = expectedAndActualTable.getOne();
            VerifiableTable actual = expectedAndActualTable.getTwo();
            comparisonResult = comparisonResult.combine(this.compare(expected, actual, skipAdaptation(expected), skipAdaptation(actual)));
        }

        boolean verificationSuccess = comparisonResult.isSuccess();

        boolean createActual = this.createActualResults;
        if (this.createActualResultsOnFailure)
        {
            createActual = !verificationSuccess;
        }
        if (createActual)
        {
            List<VerifiableTable> collect = ListIterate.collect(expectedAndActualTables, Functions.secondOfPair()).collect(this.getActualAdapter());
            this.newRebaser().rebase(this.description.getMethodName(), collect, this.getActualFile());
        }

        comparisonResult.generateBreakReport(this.description.getMethodName(), this.getOutputFile().toPath(), metadata);

        Assert.assertTrue("Some tests failed. Check test results file " + this.getOutputFile().getAbsolutePath() + " for more details.", verificationSuccess);
    }

    private boolean skipAdaptation(VerifiableTable table)
    {
        return table != null && this.tablesNotToAdapt.contains(table.getTableName());
    }

    private List<VerifiableTable> filterTables(List<VerifiableTable> tables)
    {
        return ListIterate.select(tables, table -> this.tableFilter.accept(table.getTableName()));
    }

    private void runPreVerifyChecks()
    {
        if (this.description == null)
        {
            throw new IllegalStateException("The starting() has not been called. Ensure watcher has @Rule annotation.");
        }
    }

    private void makeSureDirectoriesAreNotSame()
    {
        File expectedDirectory = this.directoryStrategy.getExpectedDirectory(this.description.getTestClass());
        File outputDirectory = this.directoryStrategy.getOutputDirectory(this.description.getTestClass());
        if (expectedDirectory != null && expectedDirectory.equals(outputDirectory))
        {
            throw new IllegalArgumentException("Expected results directory and verification output directory must NOT be the same.");
        }
    }

    private List<VerifiableTable> asList(Map<String, VerifiableTable> tables)
    {
        return Iterate.collect(tables.entrySet(), entry -> new DefaultVerifiableTableAdapter(entry.getKey(), entry.getValue()), FastList.newList());
    }

    @Override
    public Statement apply(Statement statement, Description description)
    {
        return this.bridge.apply(statement, description);
    }
}