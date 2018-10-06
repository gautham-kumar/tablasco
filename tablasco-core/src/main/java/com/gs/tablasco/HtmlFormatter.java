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

import com.gs.tablasco.compare.*;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

class HtmlFormatter
{
    public static final int DEFAULT_ROW_LIMIT = 10000;

    private static final LazyValue<DocumentBuilder> DOCUMENT_BUILDER = new LazyValue<DocumentBuilder>()
    {
        @Override
        protected DocumentBuilder initialize()
        {
            try
            {
                return DocumentBuilderFactory.newInstance().newDocumentBuilder();
            }
            catch (ParserConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
    };

    private static final LazyValue<Transformer> TRANSFORMER = new LazyValue<Transformer>()
    {
        @Override
        protected Transformer initialize()
        {
            try
            {
                Transformer transformer = TransformerFactory.newInstance().newTransformer();
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                return transformer;
            }
            catch (TransformerConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
    };

    private static final Set<Path> INITIALIZED_FILES = UnifiedSet.newSet();

    private final HtmlOptions htmlOptions;

    public HtmlFormatter(HtmlOptions htmlOptions)
    {
        this.htmlOptions = htmlOptions;
    }

    private Document initialize(Path outputPath, Metadata metadata)
    {
        if (INITIALIZED_FILES.add(outputPath))
        {
            try
            {
                Files.deleteIfExists(outputPath);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Cannot delete output file " + outputPath, e);
            }
        }
        if (Files.exists(outputPath))
        {
            try (InputStream stream = Files.newInputStream(outputPath))
            {
                return DOCUMENT_BUILDER.value().parse(stream);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error loading " + outputPath, e);
            }
        }
        return createNewDocument(metadata);
    }

    private static void ensurePathExists(Path outputPath)
    {
        Path parent = outputPath.getParent();
        if (!Files.exists(parent))
        {
            try
            {
                Files.createDirectories(parent);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Unable to create output directories for " + outputPath, e);
            }
        }
    }

    private static Document createNewDocument(Metadata metadata)
    {
        Document document = DOCUMENT_BUILDER.value().newDocument();
        Element html = document.createElement("html");
        document.appendChild(html);

        Element head = document.createElement("head");
        html.appendChild(head);

        Element script = document.createElement("script");
        script.appendChild(document.createTextNode(getVisibilityFunction()));
        head.appendChild(script);

        Element style = document.createElement("style");
        style.setAttribute("type", "text/css");
        style.appendChild(document.createTextNode(getCSSDefinitions()));
        head.appendChild(style);

        Element meta = document.createElement("meta");
        meta.setAttribute("http-equiv", "Content-type");
        meta.setAttribute("content", "text/html;charset=UTF-8");
        head.appendChild(meta);

        head.appendChild(ResultCell.createNodeWithText(document, "title", "Test Results"));

        Element body = document.createElement("body");
        html.appendChild(body);

        Element div = document.createElement("div");
        div.setAttribute("class", "metadata");
        if (metadata != null)
        {
            div.appendChild(ResultCell.createNodeWithText(document, "i", metadata.toString()));
        }
        body.appendChild(div);

        return document;
    }

    public void appendResults(Path outputPath, String testName, Map<String, ? extends FormattableTable> results, Metadata metadata)
    {
        this.appendResults(outputPath, testName, results, metadata, 1);
    }

    public void appendResults(Path outputPath, String testName, Map<String, ? extends FormattableTable> results, Metadata metadata, int verifyCount)
    {
        Map<String, FormattableTable> resultsToFormat = new LinkedHashMap<>();
        for (String name : results.keySet())
        {
            FormattableTable formattableTable = results.get(name);
            boolean dontFormat = this.htmlOptions.isHideMatchedTables() && formattableTable.isSuccess();
            if (!dontFormat)
            {
                resultsToFormat.put(name, formattableTable);
            }
        }
        if (!resultsToFormat.isEmpty())
        {
            Document dom = this.initialize(outputPath, metadata);
            ensurePathExists(outputPath);
            try (OutputStream outputStream = Files.newOutputStream(outputPath))
            {
                appendResults(testName, resultsToFormat, metadata, verifyCount, dom, outputStream);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void appendResults(String testName, Map<String, ? extends FormattableTable> results, Metadata metadata, int verifyCount, Document dom, OutputStream outputStream) throws TransformerException, UnsupportedEncodingException
    {
        if (dom == null)
        {
            dom = createNewDocument(metadata);
        }
        Node body = dom.getElementsByTagName("body").item(0);
        if (verifyCount == 1)
        {
            body.appendChild(ResultCell.createNodeWithText(dom, "h1", testName));
        }

        if (this.htmlOptions.isDisplayAssertionSummary())
        {
            appendAssertionSummary(testName, results, body);
        }
        for (Map.Entry<String, ? extends FormattableTable> namedTable : results.entrySet())
        {
            appendResults(testName, namedTable.getKey(), namedTable.getValue(), body, true);
        }
        TRANSFORMER.value().transform(new DOMSource(dom), new StreamResult(new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))));
    }

    private void appendAssertionSummary(String testName, Map<String, ? extends FormattableTable> results, Node htmlBody)
    {
        int right = 0;
        int total = 0;
        for (FormattableTable table : results.values())
        {
            right += table.getPassedCellCount();
            total += table.getTotalCellCount();
        }
        double pctCorrect = Math.floor(1000.0 * right / total) / 10;
        String cellText = String.format("%d right, %d wrong, %.1f", right, total - right, pctCorrect) + "% correct";
        ResultCell cell = ResultCell.createCustomCell(cellText, right == total ? "pass" : "fail");
        appendResults(testName, "Assertions", new ResultTable(new boolean[]{true}, Collections.singletonList(Collections.singletonList(cell))), htmlBody, false);
    }

    private void appendResults(String testName, String tableName, FormattableTable resultTable, Node htmlBody, boolean withDivId)
    {
        Element table = getTableElement(testName, tableName, htmlBody, withDivId);
        resultTable.appendTo(testName, tableName, table, this.htmlOptions);
    }

    private Element getTableElement(String testName, String tableName, Node htmlBody, boolean withDivId)
    {
        Document document = htmlBody.getOwnerDocument();
        Element div = document.createElement("div");
        if (withDivId)
        {
            div.setAttribute("id", HtmlFormatterUtils.toHtmlId(testName, tableName));
        }
        htmlBody.appendChild(div);

        div.appendChild(ResultCell.createNodeWithText(document, "h2", tableName));

        Element table = document.createElement("table");
        table.setAttribute("border", "1");
        table.setAttribute("cellspacing", "0");
        div.appendChild(table);
        return table;
    }

    private static String getVisibilityFunction()
    {
        return "\n" +
                "function toggleVisibility(id){\n" +
                "var summary = document.getElementById(id);\n" +
                "if (summary.style.display === 'none') {\n" +
                "summary.style.display = 'table-row';\n" +
                "} else {\n" +
                "summary.style.display = 'none';\n" +
                "}\n" +
                "}\n";
    }

    private static String getCSSDefinitions()
    {
        return "\n" +
                "* { padding: 0;margin: 0; }\n" +
                "body { color: black; padding: 4px; font-family: Verdana, Geneva, sans-serif; }\n" +
                "table { border-collapse: collapse; border: 0px; margin-bottom: 12px; }\n" +
                "th { font-weight: bold; }\n" +
                "td, th { white-space: nowrap; border: 1px solid black; vertical-align: top; font-size: small; padding: 2px; }\n" +
                ".pass { background-color: #c0ffc0; }\n" +
                ".fail { background-color: #ff8080; }\n" +
                ".outoforder { background-color: #d0b0ff; }\n" +
                ".missing { background-color: #cccccc; }\n" +
                ".surplus { background-color: #ffffcc; }\n" +
                ".summary { background-color: #f3f6f8; }\n" +
                ".number { text-align: right; }\n" +
                ".metadata { margin-bottom: 12px; }\n" +
                ".multi { font-style: italic; }\n" +
                ".blank_row { height: 10px; border: 0px; background-color: #ffffff; }\n" +
                ".grey { color: #999999; }\n" +
                ".blue { color: blue; }\n" +
                ".italic { font-style: italic; }\n" +
                ".link { color: blue; text-decoration: underline; cursor:pointer; font-style: italic }\n" +
                ".small { font-size: x-small; }\n" +
                "hr { border: 0px; color: black; background-color: black; height: 1px; margin: 2px 0px 2px 0px; }\n" +
                "p { font-style: italic; font-size: x-small; color: blue; padding: 3px 0 0 0; }\n" +
                "h1 { font-size: medium; margin-bottom: 4px; }\n" +
                "h2 { font-size: small; margin-bottom: 4px; }\n";
    }

    static abstract class LazyValue<T>
    {
        private T value;

        protected abstract T initialize();

        public T value()
        {
            if (value == null)
            {
                value = initialize();
            }
            return value;
        }
    }
}
