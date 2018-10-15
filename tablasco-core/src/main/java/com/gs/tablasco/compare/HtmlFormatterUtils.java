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

import com.gs.tablasco.HtmlOptions;
import org.eclipse.collections.impl.utility.StringIterate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.util.List;

public class HtmlFormatterUtils
{
    static void appendHeaderRow(Node table, FormattableTable resultTable, HtmlOptions htmlOptions)
    {
        final Element tr = table.getOwnerDocument().createElement("tr");
        table.appendChild(tr);
        List<ResultCell> headers = resultTable.getHeaders();
        for (int col = 0; col < headers.size(); col++)
        {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell;
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0)
            {
                resultCell = ResultCell.createCustomCell(String.format("%d matched columns", matchedAhead + 1), "...", "pass multi");
                col += matchedAhead;
            }
            else
            {
                resultCell = headers.get(col);
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), true);
            tr.appendChild(cell);
        }
    }

    static void appendSpanningRow(Node table, FormattableTable resultTable, String cssClass, String data, String onDataClick)
    {
        Document document = table.getOwnerDocument();

        Element tr = document.createElement("tr");
        if (onDataClick != null)
        {
            tr.setAttribute("onclick", onDataClick);
        }
        table.appendChild(tr);

        Element td = document.createElement("td");
        td.setAttribute("class", cssClass);
        td.setAttribute("colspan", String.valueOf(resultTable.getHeaders().size()));
        if (data != null)
        {
            Element nodeWithText = ResultCell.createNodeWithText(document, "a", data, "link");
            td.appendChild(nodeWithText);
        }
        tr.appendChild(td);
    }

    static void appendDataRow(Element table, FormattableTable resultTable, String rowId, String rowStyle, List<ResultCell> resultCells, HtmlOptions htmlOptions)
    {
        Element tr = table.getOwnerDocument().createElement("tr");
        if (rowId != null)
        {
            tr.setAttribute("id", rowId);
        }
        if (rowStyle != null)
        {
            tr.setAttribute("style", rowStyle);
        }
        table.appendChild(tr);
        for (int col = 0; col < resultCells.size(); col++)
        {
            int matchedAhead = resultTable.getMatchedColumnsAhead(col);
            ResultCell resultCell = resultCells.get(col);
            if (htmlOptions.isHideMatchedColumns() && matchedAhead > 0)
            {
                resultCell = ResultCell.createCustomCell("\u00A0", resultCell.getCssClass());
                col += matchedAhead;
            }
            Node cell = resultCell.createCell(tr.getOwnerDocument(), false);
            tr.appendChild(cell);
        }
    }

    public static String toHtmlId(String testName, String tableName)
    {
        if (StringIterate.isEmpty(tableName))
        {
            return testName;
        }
        return testName.replaceAll("\\W+", "_") + '.' + tableName.replaceAll("\\W+", "_");
    }

    static void appendMultiMatchedRow(Element table, int colspan, int matchedRows)
    {
        Document document = table.getOwnerDocument();
        Element tr = document.createElement("tr");
        table.appendChild(tr);
        Element td = document.createElement("td");
        td.setAttribute("class", "pass multi");
        td.setAttribute("colspan", String.valueOf(colspan));
        td.appendChild(document.createTextNode(matchedRows + ResultCell.adaptOnCount(matchedRows, " matched row") + "..."));
        tr.appendChild(td);
    }
}
