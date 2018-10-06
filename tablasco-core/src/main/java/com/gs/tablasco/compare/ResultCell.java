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

package com.gs.tablasco.compare;

import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.utility.MapIterate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.Serializable;
import java.text.NumberFormat;
import java.util.Map;

/**
 * Bean which holds result information for each cell in the grid, including formatted strings for lhs and rhs values and a
 * CellState indicating whether the cell is matched, unmatched, missing, or surplus.
 */

public abstract class ResultCell implements Serializable
{
    public static final Predicate<ResultCell> IS_FAILED_CELL = FailedCell.class::isInstance;
    public static final Predicate<ResultCell> IS_PASSED_CELL = PassedCell.class::isInstance;

    protected final CellFormatter formatter;

    protected ResultCell(CellFormatter formatter)
    {
        this.formatter = formatter;
    }

    public abstract Object getLhs();

    public abstract Object getRhs();

    public abstract String getCssClass();

    public abstract Object getSummary();

    public static ResultCell createMatchedCell(CellComparator cellComparator, Object rhs, Object lhs)
    {
        if (cellComparator.equals(rhs, lhs))
        {
            return new PassedCell(cellComparator.getFormatter(), rhs);
        }
        return new FailedCell(cellComparator.getFormatter(), rhs, lhs);
    }

    public static ResultCell createMissingCell(CellFormatter formatter, Object lhs)
    {
        return new MissingCell(formatter, lhs);
    }

    public static ResultCell createSurplusCell(CellFormatter formatter, Object rhs)
    {
        return new SurplusCell(formatter, rhs);
    }

    public static ResultCell createCustomCell(String contents, String cssClass)
    {
        return createCustomCell(null, contents, cssClass);
    }

    public static ResultCell createCustomCell(String title, String contents, String cssClass)
    {
        return new CustomCell(title, contents, cssClass);
    }

    public static ResultCell createOutOfOrderCell(CellFormatter formatter, Object rhsAndLhs)
    {
        return new OutOfOrderCell(formatter, rhsAndLhs);
    }

    public static ResultCell createSummaryCell(int maximumCardinalityToCount, ColumnCardinality columnCardinality)
    {
        return new SummaryCell(maximumCardinalityToCount, columnCardinality);
    }

    public static Element createNodeWithText(Document document, String tagName, String content)
    {
        return createNodeWithText(document, tagName, content, null);
    }

    public static Element createNodeWithText(Document document, String tagName, String content, String cssClass)
    {
        Element element = document.createElement(tagName);
        if (cssClass != null)
        {
            element.setAttribute("class", cssClass);
        }
        element.appendChild(document.createTextNode(content));
        return element;
    }

    public static Element createCell(Document document, String className, boolean headerRow, boolean isNumeric, Node... content)
    {
        Element td = document.createElement(headerRow ? "th" : "td");
        td.setAttribute("class", className + (isNumeric ? " number" : ""));
        for (Node node : content)
        {
            td.appendChild(node);
        }
        return td;
    }

    public static String adaptOnCount(int count, String s)
    {
        return count > 1 ? s + 's' : s;
    }

    public boolean isMatch()
    {
        return false;
    }

    public abstract Node createCell(Document document, boolean isHeaderRow);

    @Override
    public final boolean equals(Object o)
    {
        return this.toString().equals(String.valueOf(o));
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("equals() is only implemented for tests");
    }

    private static class PassedCell extends OutOfOrderCell
    {
        private PassedCell(CellFormatter formatter, Object rhsAndLhs)
        {
            super(formatter, rhsAndLhs);
        }

        @Override
        public boolean isMatch()
        {
            return true;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.rhsAndLhs),
                    document.createTextNode(this.formatter.format(this.rhsAndLhs)));
        }

        @Override
        public String getCssClass()
        {
            return "pass";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.rhsAndLhs);
        }
    }

    private static class FailedCell extends ResultCell
    {
        private final Object rhs;
        private final Object lhs;

        private FailedCell(CellFormatter formatter, Object rhs, Object lhs)
        {
            super(formatter);
            this.rhs = rhs;
            this.lhs = lhs;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{rhs=" + this.formatter.format(this.rhs) + ", lhs=" + this.formatter.format(this.lhs) + '}';
        }

        @Override
        public Object getLhs()
        {
            return this.lhs;
        }

        @Override
        public Object getRhs()
        {
            return this.rhs;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            boolean isRhsAndLhsNumber = CellFormatter.isNumber(this.rhs) && CellFormatter.isNumber(this.lhs);
            if (isRhsAndLhsNumber)
            {
                String difference = this.formatter.format(ToleranceCellComparator.getDifference(this.rhs, this.lhs));
                String variance = this.formatter.format(VarianceCellComparator.getVariance(this.rhs, this.lhs));
                return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, true,
                        document.createTextNode(this.formatter.format(this.lhs)),
                        ResultCell.createNodeWithText(document, "p", "Lhs"),
                        document.createElement("hr"),
                        document.createTextNode(this.formatter.format(this.rhs)),
                        ResultCell.createNodeWithText(document, "p", "Rhs"),
                        document.createElement("hr"),
                        document.createTextNode(difference + " / " + variance + '%'),
                        ResultCell.createNodeWithText(document, "p", "Difference / Variance"));
            }
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, false,
                    document.createTextNode(this.formatter.format(this.lhs)),
                    ResultCell.createNodeWithText(document, "p", "Lhs"),
                    document.createElement("hr"),
                    document.createTextNode(this.formatter.format(this.rhs)),
                    ResultCell.createNodeWithText(document, "p", "Rhs"));
        }

        @Override
        public String getCssClass()
        {
            return "fail";
        }

        @Override
        public Object getSummary()
        {
            return Maps.fixedSize.of("Lhs", this.formatter.format(this.lhs), "Rhs", this.formatter.format(this.rhs));
        }
    }

    private static class MissingCell extends ResultCell
    {
        private final Object lhs;

        private MissingCell(CellFormatter formatter, Object lhs)
        {
            super(formatter);
            this.lhs = lhs;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{lhs=" + this.formatter.format(this.lhs) + '}';
        }

        @Override
        public Object getLhs()
        {
            return this.lhs;
        }

        @Override
        public Object getRhs()
        {
            return null;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.lhs),
                    document.createTextNode(this.formatter.format(this.lhs)),
                    ResultCell.createNodeWithText(document, "p", "Missing"));
        }

        @Override
        public String getCssClass()
        {
            return "missing";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.lhs);
        }
    }

    private static class SurplusCell extends ResultCell
    {
        private final Object rhs;

        private SurplusCell(CellFormatter formatter, Object rhs)
        {
            super(formatter);
            this.rhs = rhs;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{rhs=" + this.formatter.format(this.rhs) + '}';
        }

        @Override
        public Object getLhs()
        {
            return null;
        }

        @Override
        public Object getRhs()
        {
            return this.rhs;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.rhs),
                    document.createTextNode(this.formatter.format(this.rhs)),
                    ResultCell.createNodeWithText(document, "p", "Surplus"));
        }

        @Override
        public String getCssClass()
        {
            return "surplus";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.rhs);
        }
    }

    private static class OutOfOrderCell extends ResultCell
    {
        protected final Object rhsAndLhs;

        private OutOfOrderCell(CellFormatter formatter, Object rhsAndLhs)
        {
            super(formatter);
            this.rhsAndLhs = rhsAndLhs;
        }

        @Override
        public String toString()
        {
            return this.getClass().getSimpleName() + "{rhsAndLhs=" + this.formatter.format(this.rhsAndLhs) + '}';
        }

        @Override
        public Object getLhs()
        {
            return this.rhsAndLhs;
        }

        @Override
        public Object getRhs()
        {
            return this.rhsAndLhs;
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            return ResultCell.createCell(document, this.getCssClass(), isHeaderRow, CellFormatter.isNumber(this.rhsAndLhs),
                    document.createTextNode(this.formatter.format(this.rhsAndLhs)),
                    ResultCell.createNodeWithText(document, "p", "Out of order"));
        }

        @Override
        public String getCssClass()
        {
            return "outoforder";
        }

        @Override
        public Object getSummary()
        {
            return this.formatter.format(this.rhsAndLhs);
        }
    }

    private static class SummaryCell extends ResultCell
    {
        private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
        private int maximumCardinalityToCount;
        private ColumnCardinality columnCardinality;

        private SummaryCell(int maximumCardinalityToCount, ColumnCardinality columnCardinality)
        {
            super(null);
            this.maximumCardinalityToCount = maximumCardinalityToCount;
            this.columnCardinality = columnCardinality;
        }

        @Override
        public Object getLhs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getRhs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getCssClass()
        {
            return "summary";
        }

        @Override
        public String getSummary()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node createCell(final Document document, boolean isHeaderRow)
        {
            final Element node = document.createElement("td");
            node.setAttribute("class", this.getCssClass() + " small");
            if (this.columnCardinality.isFull() || this.columnCardinality.getDistinctCount() > this.maximumCardinalityToCount)
            {
                node.appendChild(ResultCell.createNodeWithText(document, "span", ">" + NUMBER_FORMAT.format(this.maximumCardinalityToCount) + " distinct values", "italic"));
            }
            else
            {
                this.columnCardinality.forEachWithOccurrences((Procedure2<Object, Integer>) (value, occurrences) ->
                {
                    if (value instanceof Map)
                    {
                        Map valueMap = (Map) value;
                        MapIterate.forEachKeyValue(valueMap, (Procedure2) (type, value1) ->
                        {
                            node.appendChild(ResultCell.createNodeWithText(document, "span", String.valueOf(type) + " ", "grey"));
                            node.appendChild(getValueNode(document, value1));
                        });
                    }
                    else
                    {
                        node.appendChild(getValueNode(document, value));
                    }
                    node.appendChild(ResultCell.createNodeWithText(document, "span", "- ", "grey"));
                    node.appendChild(ResultCell.createNodeWithText(document, "span", NUMBER_FORMAT.format(occurrences) + adaptOnCount(occurrences, " row"), "italic blue"));
                    node.appendChild(document.createElement("br"));
                });
            }
            return node;
        }

        private Node getValueNode(Document document, Object value)
        {
            return document.createTextNode(String.valueOf(value) + " ");
        }
    }

    private static class CustomCell extends ResultCell
    {
        private final String title;
        private final String cell;
        private final String cssClass;

        private CustomCell(String title, String cell, String cssClass)
        {
            super(null);
            this.title = title;
            this.cell = cell;
            this.cssClass = cssClass;
        }

        @Override
        public Object getLhs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getRhs()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node createCell(Document document, boolean isHeaderRow)
        {
            Element th = document.createElement(isHeaderRow ? "th" : "td");
            th.setAttribute("class", this.getCssClass());
            if (this.title != null)
            {
                th.setAttribute("title", this.title);
            }
            th.appendChild(document.createTextNode(this.cell));
            return th;
        }

        @Override
        public String getCssClass()
        {
            return this.cssClass;
        }

        @Override
        public String getSummary()
        {
            if (this.title != null)
            {
                return (this.title + ": " + this.cell);
            }
            return this.cell;
        }
    }
}
