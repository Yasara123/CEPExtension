package org.wso2.siddhi.extension.CustomExeFun;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class PageTitle extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PageTitle.class);
    private VariableExpressionExecutor pageURL;
    private ComplexEventChunk<StreamEvent> returnEventChunk;
    private final int corePoolSize = 8;
    private final int maxPoolSize = 300;
    private final long keepAliveTime = 20000;
    private final int jobQueueSize = 10000;
    private BlockingQueue<Runnable> BlockingQ;
    private ExecutorService threadExecutor;

    @Override
    public void start() {
       
        BlockingQ = new LinkedBlockingQueue<Runnable>(jobQueueSize);
        threadExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                BlockingQ);

    }

    @Override
    public void stop() {
       
        threadExecutor.shutdown();
        BlockingQ.clear();
    }

    @Override
    public Object[] currentState() {
      
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
       

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
        StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {  
        returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            threadExecutor.submit(new  LinkConnector(streamEvent, complexEventPopulater));

        }
        sendEventChunk();

    }
    public synchronized void addEventChunk(StreamEvent event) {
        returnEventChunk.add(event);
    }
    public synchronized void sendEventChunk() {
        if (returnEventChunk.hasNext()) {
            nextProcessor.process(returnEventChunk);
            returnEventChunk = new ComplexEventChunk<StreamEvent>();
        }
    }
    private String getLink(Document doc, StringBuilder sb) {
        String text = null;
        Elements metaOgTitle = doc.select("meta[property=og:title]");
        if (metaOgTitle != null) {
            text = metaOgTitle.attr("content");
        } else {
            text = doc.title();
        }
        if (text == "") {
            text = doc.title();
        }
        if (text != null) {
            sb.append(text);
        }
        return sb.toString();
    }

   
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (attributeExpressionExecutors.length != 1) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to GetPost() function, "
                    + "required 1, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            pageURL = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new UnsupportedOperationException("The first parameter should be an keyword String");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Title", Attribute.Type.STRING));
        return attributeList;
    }

    class  LinkConnector implements Runnable {
        private String linkPageURL;
        StreamEvent event;
        ComplexEventPopulater complexEventPopulater;

        public LinkConnector(StreamEvent event, ComplexEventPopulater complexEventPopulater) {
            this.linkPageURL = (String) pageURL.execute(event);
            this.event = event;
            this.complexEventPopulater = complexEventPopulater;
        }

        @Override
        public void run() {
         
            StringBuilder sb = new StringBuilder();
            Connection con = Jsoup
                    .connect(linkPageURL)
                    .userAgent(
                            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                    .ignoreContentType(true).ignoreHttpErrors(true).timeout(10000);
            Document doc;
            try {
                doc = con.get();
                String Title = getLink(doc, sb);             
                complexEventPopulater.populateComplexEvent(event, new Object[] { Title });
                addEventChunk(event);
            } catch (IOException e) {
              //todo:
                logger.error("Error Extracting Link Data in PageTitle extension ", e);
            }

        }

    }

}
