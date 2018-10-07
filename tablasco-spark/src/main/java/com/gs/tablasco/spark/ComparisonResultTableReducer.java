package com.gs.tablasco.spark;


import com.gs.tablasco.ComparisonResult;
import org.apache.spark.api.java.function.Function2;

public class ComparisonResultTableReducer implements Function2<ComparisonResult, ComparisonResult, ComparisonResult>
{
    @Override
    public ComparisonResult call(ComparisonResult t1, ComparisonResult t2)
    {
        return t1.combine(t2, true);
    }
}
