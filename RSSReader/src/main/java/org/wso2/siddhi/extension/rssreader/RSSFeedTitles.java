package org.wso2.siddhi.extension.rssreader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class RSSFeedTitles extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RSSFeedTitles.class);
    private String urlSting;
    private URL rssURL;
    private int passToOut;
    private String[] titles;
    private String[] pubDates;
    private String[] discriptions;
    private String[] links;

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
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent = streamEventChunk.getFirst();
        if (urlSting != null) {
            if (((String) urlSting).startsWith("https:")) {
                try {
                    setURL(new URL((String) urlSting));

                    readFeed();
                    int min = (passToOut > titles.length) ? titles.length : passToOut;
                    for (int i = 0; i < min; i++) {
                        StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { titles[i],
                                discriptions[i], pubDates[i], links[i], i + 1 });
                        returnEventChunk.add(clonedEvent);
                        nextProcessor.process(returnEventChunk);
                    }
                } catch (Exception e) {
                    LOGGER.error("error read RSS feeds in RSSReedTitles class " + e);
                }
            } else {
                throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function should contain URL");
            }
        } else {
            throw new ExecutionPlanRuntimeException("Input to the RSS:Reader() function cannot be null");
        }

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to math:sin() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            urlSting = ((String) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an String");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            passToOut = (Integer) (attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The 2 parameter should be an integer");
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Title", Attribute.Type.STRING));
        attributeList.add(new Attribute("Dis", Attribute.Type.STRING));
        attributeList.add(new Attribute("Pub", Attribute.Type.STRING));
        attributeList.add(new Attribute("Link", Attribute.Type.STRING));
        attributeList.add(new Attribute("Count", Attribute.Type.INT));
        return attributeList;
    }

    // todo:remove
    public void setURL(URL url) {
        rssURL = url;
    }

    public void readFeed() {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(rssURL.openStream());
            NodeList items = doc.getElementsByTagName("item");
            titles = new String[items.getLength()];
            pubDates = new String[items.getLength()];
            discriptions = new String[items.getLength()];
            links = new String[items.getLength()];
            for (int i = 0; i < items.getLength(); i++) {
                Element item = (Element) items.item(i);
                titles[i] = (String) getValue(item, "title");
                pubDates[i] = (String) getValue(item, "pubDate");
                discriptions[i] = (String) getValue(item, "description");
                links[i] = (String) getValue(item, "link");
            }

        } catch (Exception e) {
            LOGGER.error("error read RSS feeds in readFeed function in RSSReedTitles class " + e);
        }

    }

    public String getValue(Element parent, String nodeName) {
        return parent.getElementsByTagName(nodeName).item(0).getFirstChild().getNodeValue();
    }

}
