package com.gs.tablasco;

import com.gs.tablasco.compare.DefaultComparableTableAdapter;
import org.eclipse.collections.api.block.function.Function;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

public class TableComparatorTest
{
    @Rule
    public final TableTestUtils.TestInputReader inputReader = new TableTestUtils.TestInputReader(
            new File("src/test/resources/" + TableComparatorTest.class.getSimpleName() + ".txt"));

    @Test
    public void validationSuccess()
    {
        ComparisonResult result = this.constructComparator().compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void validationFailure()
    {
        ComparisonResult result = this.constructComparator().compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void toleranceSuccess()
    {
        ComparisonResult result = this.constructComparator().withTolerance(0.2d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void toleranceSuccessForFirstColumn()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Age", 0.2d).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void toleranceSuccessForSecondColumn()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Weight", 0.06d).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void toleranceSuccessForTwoColumns()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Weight", 0.06d).withTolerance("Age", 0.2d).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void toleranceSuccessWithGeneralCase()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Weight", 0.06d).withTolerance(1.0d).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void toleranceFailure()
    {
        ComparisonResult result = this.constructComparator().withTolerance(0.1d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void toleranceFailureForTwoColumns()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Age", 0.2d).withTolerance("Weight", 0.06d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void toleranceFailureWithGeneralCase()
    {
        ComparisonResult result = this.constructComparator().withTolerance("Weight", 0.06d).withTolerance(1.0d).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void varianceSuccess()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold(5.0d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void varianceSuccessForTwoColumns()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold("Weight", 1.0d).withVarianceThreshold("Age", 5.0d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void varianceSuccessWithTolerance()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold("Weight", 1.0d).withVarianceThreshold("Age", 5.0d)
                .withTolerance("Age", 0.2d).withTolerance("Weight", 0.06d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void varianceFailure()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold(5.0d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void varianceFailureForTwoColumns()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold("Age", 5.0d).withVarianceThreshold("Weight", 1.0d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_3);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void mismatchedTypesFormatting()
    {
        ComparisonResult result = this.constructComparator().withVarianceThreshold(5.0d).withCompareRowOrder(true).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void rowOrderSuccess()
    {
        ComparisonResult result = this.constructComparator().withTolerance(1.0).withCompareRowOrder(false).compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void rowOrderFailure()
    {
        ComparisonResult result = this.constructComparator().compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertFalse(result.isSuccess());
    }

    @Test
    public void withoutPartialMatchTimeout()
    {
        TableComparator comparator = this.constructComparator();
        comparator.withoutPartialMatchTimeout();
        ComparisonResult result = comparator.compare(TableTestUtils.TEST_DATA_1, TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void rhsAdapter()
    {
        ComparisonResult result = this.constructComparator().withRhsAdapter(RHS_ADAPTER)
                .compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void rhsAdapterWithTestData()
    {
        TableComparator comparator = this.constructComparator().withRhsAdapter(rhs ->
        {
            Assert.assertSame(TableTestUtils.TEST_DATA_2, rhs);
            return TableTestUtils.TEST_DATA_1;
        });

        ComparisonResult result = comparator.compare(TableTestUtils.TEST_DATA_1, TableTestUtils.TEST_DATA_2);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void lhsAdapter()
    {
        ComparisonResult result = this.constructComparator().withLhsAdapter(LHS_ADAPTER)
                .compare(this.getLhsTableForTest(TableTestUtils.TABLE_NAME), TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    public void lhsAdapterNoExpectedFile()
    {
        TableComparator comparator = this.constructComparator().withLhsAdapter(lhs ->
        {
            Assert.assertSame(TableTestUtils.TEST_DATA_2, lhs);
            return TableTestUtils.TEST_DATA_1;
        });

        ComparisonResult result = comparator.compare(TableTestUtils.TEST_DATA_2, TableTestUtils.TEST_DATA_1);
        Assert.assertTrue(result.isSuccess());
    }

    private static final Function<ComparableTable, ComparableTable> LHS_ADAPTER = new Function<ComparableTable, ComparableTable>()
    {
        @Override
        public ComparableTable valueOf(ComparableTable rhs)
        {
            return new DefaultComparableTableAdapter(rhs)
            {
                @Override
                public String getColumnName(int columnIndex)
                {
                    return super.getColumnName(columnIndex).substring(8); // strip "Ignored "
                }
            };
        }
    };

    private static final Function<ComparableTable, ComparableTable> RHS_ADAPTER = new Function<ComparableTable, ComparableTable>()
    {
        @Override
        public ComparableTable valueOf(ComparableTable rhs)
        {
            return new DefaultComparableTableAdapter(rhs)
            {
                @Override
                public String getColumnName(int columnIndex)
                {
                    return "Adapted " + super.getColumnName(columnIndex);
                }
            };
        }
    };

    private TableComparator constructComparator()
    {
        return new TableComparator();
    }

    private ComparableTable getLhsTableForTest(String tableName)
    {
        return this.inputReader.getLhsTableForTest(tableName);
    }
}