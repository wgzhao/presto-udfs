/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wgzhao.presto.udfs.utils;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Utility class for all XPath UDFs. Each UDF instance should keep an instance
 * of this class.
 */
public class UDFXPathUtil
{
    public static final String SAX_FEATURE_PREFIX = "http://xml.org/sax/features/";
    public static final String EXTERNAL_GENERAL_ENTITIES_FEATURE = "external-general-entities";
    public static final String EXTERNAL_PARAMETER_ENTITIES_FEATURE = "external-parameter-entities";
    //    private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
//    private DocumentBuilder builder = null;
    private final XPath xpath = XPathFactory.newInstance().newXPath();

    private static final Object mutex = new Object();
    private static volatile UDFXPathUtil instance;
    private XPathExpression expression = null;
    private String oldPath = null;

    private static final ThreadLocal<DocumentBuilder> docBuilderIns = ThreadLocal.withInitial(() -> {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature(SAX_FEATURE_PREFIX + EXTERNAL_GENERAL_ENTITIES_FEATURE, false);
            dbf.setFeature(SAX_FEATURE_PREFIX + EXTERNAL_PARAMETER_ENTITIES_FEATURE, false);
            return dbf.newDocumentBuilder();
        }
        catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
    });

    public static UDFXPathUtil getInstance()
    {
        UDFXPathUtil result = instance;
        if (result == null) {
            synchronized (mutex) {
                result = instance;
                if (result == null) {
                    instance = result = new UDFXPathUtil();
                }
            }
            return result;
        }
        return instance;
    }

    public Object eval(String xml, String path, QName qname)
    {
        if (xml == null || path == null || qname == null) {
            return null;
        }

        if (xml.length() == 0 || path.length() == 0) {
            return null;
        }

        if (!path.equals(oldPath)) {
            try {
                expression = xpath.compile(path);
            }
            catch (XPathExpressionException e) {
                expression = null;
            }
            oldPath = path;
        }

        if (expression == null) {
            return null;
        }

        ReusableStringReader reader = new ReusableStringReader();
        InputSource inputSource = new InputSource(reader);
        reader.set(xml);
        try {
            return expression.evaluate(docBuilderIns.get().parse(inputSource), qname);
        }
        catch (XPathExpressionException e) {
            throw new RuntimeException("Invalid expression '" + oldPath + "'", e);
        }
        catch (Exception e) {
            throw new RuntimeException("Error loading expression '" + oldPath + "'", e);
        }
    }

    public Boolean evalBoolean(String xml, String path)
    {
        return (Boolean) eval(xml, path, XPathConstants.BOOLEAN);
    }

    public String evalString(String xml, String path)
    {
        return (String) eval(xml, path, XPathConstants.STRING);
    }

    public Double evalNumber(String xml, String path)
    {
        return (Double) eval(xml, path, XPathConstants.NUMBER);
    }

    public Node evalNode(String xml, String path)
    {
        return (Node) eval(xml, path, XPathConstants.NODE);
    }

    public NodeList evalNodeList(String xml, String path)
    {
        return (NodeList) eval(xml, path, XPathConstants.NODESET);
    }

    /**
     * Reusable, non-thread-safe version of {@link StringReader}.
     */
    public static class ReusableStringReader
            extends Reader
    {

        private String str = null;
        private int length = -1;
        private int next = 0;
        private int mark = 0;

        public ReusableStringReader()
        {
        }

        public void set(String s)
        {
            this.str = s;
            this.length = s.length();
            this.mark = 0;
            this.next = 0;
        }

        /**
         * Check to make sure that the stream has not been closed
         */
        private void ensureOpen()
                throws IOException
        {
            if (str == null) {
                throw new IOException("Stream closed");
            }
        }

        @Override
        public int read()
                throws IOException
        {
            ensureOpen();
            if (next >= length) {
                return -1;
            }
            return str.charAt(next++);
        }

        @Override
        public int read(char cbuf[], int off, int len)
                throws IOException
        {
            ensureOpen();
            if ((off < 0) || (off > cbuf.length) || (len < 0)
                    || ((off + len) > cbuf.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            }
            else if (len == 0) {
                return 0;
            }
            if (next >= length) {
                return -1;
            }
            int n = Math.min(length - next, len);
            str.getChars(next, next + n, cbuf, off);
            next += n;
            return n;
        }

        @Override
        public long skip(long ns)
                throws IOException
        {
            ensureOpen();
            if (next >= length) {
                return 0;
            }
            // Bound skip by beginning and end of the source
            long n = Math.min(length - next, ns);
            n = Math.max(-next, n);
            next += n;
            return n;
        }

        @Override
        public boolean ready()
                throws IOException
        {
            ensureOpen();
            return true;
        }

        @Override
        public boolean markSupported()
        {
            return true;
        }

        @Override
        public void mark(int readAheadLimit)
                throws IOException
        {
            if (readAheadLimit < 0) {
                throw new IllegalArgumentException("Read-ahead limit < 0");
            }
            ensureOpen();
            mark = next;
        }

        @Override
        public void reset()
                throws IOException
        {
            ensureOpen();
            next = mark;
        }

        @Override
        public void close()
        {
            str = null;
        }
    }
}