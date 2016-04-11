package org.wso2.siddhi.extension.twittersentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import com.clearspring.analytics.stream.ITopK;
import com.clearspring.analytics.stream.ScoredItem;

public class WordCloudSlidingWindow extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCloudSlidingWindow.class);
    private int passToOut = 50;
    private int maxLength = 500;
    private static VariableExpressionExecutor varibleExecutorText;
    private static CustomConcurrentStreamSummary<String> topKWindow1;
    private static CustomConcurrentStreamSummary<String> topKWindow2;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                topKWindow1 = new CustomConcurrentStreamSummary<String>(maxLength);
                LOGGER.info("Window 1 start at word cloud exetension");
                if (topKWindow2 != null) {
                    List<String> stuff = (List) topKWindow2.peekWithScores((int) topKWindow2.size());
                    for (int i = 0; i < (int) topKWindow2.size(); i++) {
                        JSONObject jsonObj = new JSONObject(stuff.get(i));
                        String val = jsonObj.getString("item");
                        int count = jsonObj.getInt("count");
                        topKWindow1.offer(val, count);
                    }

                }
            }
        }, 0, 16, HOURS);
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                LOGGER.info("Window 2 start at word cloud exetension");
                topKWindow2 = new CustomConcurrentStreamSummary<String>(maxLength);
            }
        }, 8, 16, HOURS);
    }

    @Override
    public void stop() {
        LOGGER.info("Schedular shutdow in word cloud extension.");
        scheduler.shutdownNow();

    }

    @Override
    public Object[] currentState() {
        Object[] arr = new Object[0];
        return arr;// No need to maintain a state.
    }

    @Override
    public void restoreState(Object[] state) {
        // No need to maintain a state
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        String rawString;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            rawString = (String) varibleExecutorText.execute(streamEvent);
            String[] arr = rawString.split(" ");

            for (int i = 0; i < arr.length; i++) {
                if ((topKWindow1 != null) && " ".equals(arr[i])) {
                    topKWindow1.offer(arr[i].trim().toLowerCase());
                }
                if ((topKWindow2 != null) && " ".equals(arr[i])) {
                    topKWindow2.offer(arr[i].trim().toLowerCase());
                }
            }
            int peek = (int) ((topKWindow1.size() < passToOut) ? topKWindow1.size() : passToOut);
            List<String> passOut = (List) topKWindow1.peekWithScores(peek);
            String processed = "";
            for (int i = 0; i < peek; i++) {
                processed = processed.concat(passOut.get(i) + ";");
            }
            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
            clonedEvent = setAttributeText(clonedEvent, processed);
            returnEventChunk.add(clonedEvent);

        }
        nextProcessor.process(returnEventChunk);
    }

    private StreamEvent setAttributeText(StreamEvent event, String val) {
        switch (varibleExecutorText.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, varibleExecutorText.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, varibleExecutorText.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, varibleExecutorText.getPosition()[3]);
            break;
        default:
            LOGGER.error("Error in update text in wordcloudslidingwndow class");
        }
        return event;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 3)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            passToOut = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            maxLength = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
            varibleExecutorText = (VariableExpressionExecutor) attributeExpressionExecutors[2];
        } else {
            throw new UnsupportedOperationException("The first parameter should be Tweet Text");
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        return attributeList;
    }
}

class CustomConcurrentStreamSummary<T> implements ITopK<T> {
    private final int capacity;
    private final Map<T, ScoredItem<T>> itemMap;
    private final AtomicReference<ScoredItem<T>> minVal;
    private final AtomicLong size;
    private final AtomicBoolean reachCapacity;

    public CustomConcurrentStreamSummary(final int capacity) {
        this.capacity = capacity;
        this.minVal = new AtomicReference<ScoredItem<T>>();
        this.size = new AtomicLong(0);
        this.itemMap = new ConcurrentHashMap<T, ScoredItem<T>>(capacity);
        this.reachCapacity = new AtomicBoolean(false);
    }

    @Override
    public boolean offer(final T element) {
        return offer(element, 1);
    }

    @Override
    public boolean offer(final T element, final int incrementCount) {
        long val = incrementCount;
        ScoredItem<T> value = new ScoredItem<T>(element, incrementCount);
        ScoredItem<T> oldVal = ((ConcurrentHashMap<T, ScoredItem<T>>) itemMap).putIfAbsent(element, value);
        if (oldVal != null) {
            val = oldVal.addAndGetCount(incrementCount);
        } else if (reachCapacity.get() || size.incrementAndGet() > capacity) {
            reachCapacity.set(true);

            ScoredItem<T> oldMinVal = minVal.getAndSet(value);
            itemMap.remove(oldMinVal.getItem());

            while (oldMinVal.isNewItem()) {
                // Wait for the oldMinVal so its error and value are completely up to date.
                // no thread.sleep here due to the overhead of calling it - the waiting time will be microseconds.
            }
            long count = oldMinVal.getCount();
            value.addAndGetCount(count);
            value.setError(count);
        }
        value.setNewItem(false);
        minVal.set(getMinValue());

        return val != incrementCount;
    }

    private ScoredItem<T> getMinValue() {
        ScoredItem<T> minValList = null;
        for (ScoredItem<T> entry : itemMap.values()) {
            if (minValList == null || (!entry.isNewItem() && entry.getCount() < minValList.getCount())) {
                minValList = entry;
            }
        }
        return minValList;
    }

    public long size() {
        return size.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ScoredItem entry : itemMap.values()) {
            sb.append("(" + entry.getCount() + ": " + entry.getItem() + ", e: " + entry.getError() + "),");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<T> peek(final int k) {
        List<T> toReturn = new ArrayList<T>(k);
        List<ScoredItem<T>> values = peekWithScores(k);
        for (ScoredItem<T> value : values) {
            toReturn.add(value.getItem());
        }
        return toReturn;
    }

    public List<ScoredItem<T>> peekWithScores(final int k) {
        List<ScoredItem<T>> values = new ArrayList<ScoredItem<T>>();
        for (Map.Entry<T, ScoredItem<T>> entry : itemMap.entrySet()) {
            ScoredItem<T> value = entry.getValue();
            values.add(new ScoredItem<T>(value.getItem(), value.getCount(), value.getError()));
        }
        Collections.sort(values);
        values = values.size() > k ? values.subList(0, k) : values;
        return values;
    }

}
