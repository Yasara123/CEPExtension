package org.wso2.siddhi.extension.rssreader;

import net.htmlparser.jericho.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.io.IOException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.jsoup.*;
import org.jsoup.select.*;

public class FullTextReader extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FullTextReader.class);
    private URL rssURL;
    private String[] titles;

    @Override
    public Type getReturnType() {
        // No need to maintain a state.
        return Attribute.Type.STRING;
    }

    @Override
    public void start() {
        // No need to maintain a state.
    }

    @Override
    public void stop() {
        // No need to maintain a state.

    }

    @Override
    public Object[] currentState() {
        Object[] arr = new Object[0];
        return arr;// No need to maintain a state.
    }

    @Override
    public void restoreState(Object[] state) {
        // No need to maintain a state.

    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to math:sin() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }

    }

    @Override
    protected Object execute(Object[] data) {
        Object arr = new Object[0];
        return arr;// No need to maintain a state.
    }

    @Override
    protected Object execute(Object data) {
        if (data != null) {
            if (((String) data).startsWith("https:")) {
                try {
                    setURL(new URL((String) data));
                    String[] links = readFeed();
                    String textField = "";
                    String formatedString = "";
                    for (int i = 0; i < links.length; i++) {
                        formatedString = textField = textField.concat("#Artical:" + String.valueOf(i + 1) + "#")
                                .concat("\n").concat(titles[i]).concat("\n").concat(readText(links[i])).concat("\n\n");
                    }
                    return formatedString;
                } catch (Exception e) {
                    LOGGER.error("error read RSS feeds in FullTextReader class " + e);
                }
            } else {
                throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function should contain URL");
            }
        } else {
            throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function cannot be null");
        }
        Object arr = new Object[0];
        return arr;// No need to maintain a state.
    }

    public String readText(String link) {
        String text = "";
        try {
            String url = link;
            org.jsoup.nodes.Document doc = (org.jsoup.nodes.Document) Jsoup.connect(url).get();
            Elements paragraphs = doc.select("p");
            for (org.jsoup.nodes.Element p : paragraphs) {
                text = text.concat(p.text());
            }

        } catch (IOException ex) {
            LOGGER.error("error read RSS feeds in ReadText method FullTextReader class " + ex);
        }

        return text;
    }

    public String clr(String htl) {
        Source source = new Source(htl);
        return source.getTextExtractor().toString();
    }

    public void setURL(URL url) {
        rssURL = url;
    }

    public String[] readFeed() {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(rssURL.openStream());
            NodeList items = doc.getElementsByTagName("item");
            String[] links = new String[items.getLength()];
            titles = new String[items.getLength()];
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                links[i] = (String) getValue(item, "link");
                titles[i] = (String) getValue(item, "title");
            }
            return links;
        } catch (Exception e) {
            LOGGER.error("error read RSS feeds in FullTextReader class " + e);
        }
        String[]arr = new String[0];
        return arr;// No need to maintain a state.
    }

    public String getValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

}
