package com.gs.tablasco.spark;

import com.gs.tablasco.ComparisonResult;
import com.gs.tablasco.compare.Metadata;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkVerifier
{
    private final List<String> groupKeyColumns;
    private final int maximumNumberOfGroups;
    private final DataFormat dataFormat;
    private boolean ignoreSurplusColumns;
    private Set<String> columnsToIgnore;
    private final Metadata metadata = Metadata.newEmpty();
    private Optional<Double> tolerance = Optional.empty();
    private final Map<String, Double> columnSpecificTolerance = UnifiedMap.newMap();

    public SparkVerifier(List<String> groupKeyColumns, int maximumNumberOfGroups, DataFormat dataFormat)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.maximumNumberOfGroups = maximumNumberOfGroups;
        this.dataFormat = dataFormat;
    }

    public final SparkVerifier withMetadata(String name, String value)
    {
        this.metadata.add(name, value);
        return this;
    }

    public SparkVerifier withIgnoreSurplusColumns(boolean ignoreSurplusColumns)
    {
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        return this;
    }

    public SparkVerifier withColumnsToIgnore(Set<String> columnsToIgnore)
    {
        this.columnsToIgnore = columnsToIgnore;
        return this;
    }

    public SparkVerifier withTolerance(double tolerance)
    {
        this.tolerance = Optional.of(tolerance);
        return this;
    }

    public SparkVerifier withTolerance(String columnName, double tolerance)
    {
        this.columnSpecificTolerance.put(columnName, tolerance);
        return this;
    }

    public SparkResult verify(String dataName, Path actualDataLocation, Path expectedDataLocation)
    {
        Set<String> groupKeyColumnSet = new LinkedHashSet<>(this.groupKeyColumns);

        Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> actualHeadersAndGroups = this.dataFormat.readHeadersAndGroups(actualDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);
        Pair<List<String>, JavaPairRDD<Integer, Iterable<List<Object>>>> expectedHeadersAndGroups = this.dataFormat.readHeadersAndGroups(expectedDataLocation, groupKeyColumnSet, this.maximumNumberOfGroups);

        JavaPairRDD<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>> joinedRdd =
                actualHeadersAndGroups.getTwo().fullOuterJoin(expectedHeadersAndGroups.getTwo());

        VerifyGroupFunction verifyGroupFunction = new VerifyGroupFunction(
                groupKeyColumnSet,
                actualHeadersAndGroups.getOne(),
                expectedHeadersAndGroups.getOne(),
                this.ignoreSurplusColumns,
                this.columnsToIgnore,
                this.tolerance,
                this.columnSpecificTolerance);

        ComparisonResult comparisonResult = joinedRdd.map(verifyGroupFunction).reduce(new ComparisonResultTableReducer());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try
        {
            comparisonResult.writeBreakReportToStream(dataName, this.metadata, outputStream);
            return new SparkResult(comparisonResult.isSuccess(), new String(outputStream.toByteArray(), "UTF-8"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
