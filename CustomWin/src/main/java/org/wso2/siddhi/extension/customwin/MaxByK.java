package org.wso2.siddhi.extension.customwin;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorDouble;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

/*
 * #Customwindow.maxByK('K', "ln(R)-kt", "*",'Tweet text','rank')
 * Sample Query:
 * from inputStream#Customwindow.maxByK(200,5, "ln(R)-kt", "*",'Tweet text','rank')
 * select attribute1, attribute2
 * insert into outputStream;
 */

public class MaxByK extends StreamProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(MaxByK.class);
    private static DivideExpressionExecutorDouble constantFunctionExecutor;
    private int passToOut;
    private int lengthtokeep;
    private List<StreamEvent> sortedWindow = new ArrayList<StreamEvent>();
    private EventComparator eventComparator;
    private VariableExpressionExecutor variableExpressionExecutor;
    private VariableExpressionExecutor variableExpressionCount;
    private VariableExpressionExecutor variableExpressionRank;
    private VariableExpressionExecutor variableExpressionRt;
    private VariableExpressionExecutor variableExpressionFt;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private class EventComparator implements Comparator<StreamEvent> {
        @Override
        public int compare(StreamEvent e1, StreamEvent e2) {
            int comparisonResult;
            int[] variablePosition = ((VariableExpressionExecutor) variableExpressionRank).getPosition();
            Comparable comparableVariable1 = (Comparable) e1.getAttribute(variablePosition);
            Comparable comparableVariable2 = (Comparable) e2.getAttribute(variablePosition);
            comparisonResult = comparableVariable1.compareTo(comparableVariable2);
            if (comparisonResult != 0) {
                return (-1) * comparisonResult;
            } else {
                return 0;
            }
        }
    }

    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                double rank = 0;
                for (int i = 0; i < sortedWindow.size(); i++) {
                    try {
                        rank = (Double) constantFunctionExecutor.execute(sortedWindow.get(i));
                    } catch (ClassCastException e) {
                        LOG.error("ClassCastException in maxByKLinks when cast rank to double of current window events "
                                + e);
                    }
                    setAttributeRank(sortedWindow.get(i), rank);
                }
                LOG.info("Calcutaled sorted window rank to recent time in maxByk");
            }

        }, 0, 5, TimeUnit.MINUTES);

    }

    @Override
    public void stop() {
        scheduler.shutdown();
    }

    @Override
    public Object[] currentState() {
        return new Object[] { sortedWindow };
    }

    @Override
    public void restoreState(Object[] state) {
        sortedWindow = (ArrayList<StreamEvent>) state[0];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        Double rank = -1.0;
        boolean addNewEvent = false;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            try {
                rank = (Double) constantFunctionExecutor.execute(streamEvent);

            } catch (ClassCastException e) {
                LOG.error("ClassCastException in maxByKLinks when cast rank to double of new event " + e);
            }
            streamEvent = setAttributeRank(streamEvent, rank);
            boolean duplicate = false;

            if (sortedWindow.size() < lengthtokeep) {
                duplicate = isDuplicate(streamEvent);
                if (duplicate == false) {
                    sortedWindow.add(streamEvent);
                    addNewEvent = true;
                    Collections.sort(sortedWindow, eventComparator);
                }
            } else if ((Double) variableExpressionRank.execute(sortedWindow.get(sortedWindow.size() - 1)) > (Double) variableExpressionRank
                    .execute(streamEvent) || "null".equals(variableExpressionExecutor.execute(streamEvent))) {
                continue;
            } else {
                duplicate = isDuplicate(streamEvent);
                if (duplicate == false) {
                    sortedWindow.remove(sortedWindow.size() - 1);
                    sortedWindow.add(streamEvent);
                    addNewEvent = true;
                    Collections.sort(sortedWindow, eventComparator);
                }
            }
        }
        if (addNewEvent) {
            for (int j = 0; j < passToOut; j++) {
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(sortedWindow.get(j));
                complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { j + 1 });
                returnEventChunk.add(clonedEvent);
            }
            nextProcessor.process(returnEventChunk);
        }

    }

    private StreamEvent setAttributeRank(StreamEvent event, double val) {
        switch (variableExpressionRank.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionRank.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionRank.getPosition()[3]);
            break;
        default:
            LOG.error("Error in update rank in maxByK class");
        }
        return event;
    }

    private StreamEvent setAttributeRt(StreamEvent event, int val) {
        switch (variableExpressionRt.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionRt.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionRt.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionRt.getPosition()[3]);
            break;
        default:
            LOG.error("Error in update retweet in maxByK class");
        }
        return event;
    }

    private StreamEvent setAttributeFt(StreamEvent event, int val) {
        switch (variableExpressionFt.getPosition()[2]) {
        case 0:
            event.setBeforeWindowData(val, variableExpressionFt.getPosition()[3]);
            break;
        case 1:
            event.setOnAfterWindowData(val, variableExpressionFt.getPosition()[3]);
            break;
        case 2:
            event.setOutputData(val, variableExpressionFt.getPosition()[3]);
            break;
        default:
            LOG.error("Error in update favourite in maxByK class");
        }
        return event;
    }

    private boolean isDuplicate(StreamEvent event) {
        boolean duplicate = false;
        for (int i = sortedWindow.size() - 1; i >= 0; i--) {
            if (variableExpressionExecutor.execute(sortedWindow.get(i)).equals(
                    variableExpressionExecutor.execute(event))) {
                if ((Double) variableExpressionRank.execute(sortedWindow.get(i)) < (Double) variableExpressionRank
                        .execute(event)) {
                    setAttributeRank(sortedWindow.get(i), (Double) variableExpressionRank.execute(event));
                }
                if ((Integer) variableExpressionRt.execute(sortedWindow.get(i)) < (Integer) variableExpressionRt
                        .execute(event)) {
                    setAttributeRt(sortedWindow.get(i), (Integer) variableExpressionRt.execute(event));
                }
                if ((Integer) variableExpressionFt.execute(sortedWindow.get(i)) < (Integer) variableExpressionFt
                        .execute(event)) {
                    setAttributeFt(sortedWindow.get(i), (Integer) variableExpressionFt.execute(event));
                }
                Collections.sort(sortedWindow, eventComparator);
                duplicate = true;
            }
        }
        return duplicate;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 8)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            lengthtokeep = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            passToOut = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (!(attributeExpressionExecutors[2] instanceof DivideExpressionExecutorDouble)) {
            throw new UnsupportedOperationException(
                    "Required a Devide Function Executor, but found a other Function Type");
        } else {
            constantFunctionExecutor = (DivideExpressionExecutorDouble) attributeExpressionExecutors[2];
        }
        if (!(attributeExpressionExecutors[3] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a string parameter");
        } else {
            variableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[3];
        }
        if (!(attributeExpressionExecutors[4] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionRank = (VariableExpressionExecutor) attributeExpressionExecutors[4];
        }
        if (!(attributeExpressionExecutors[5] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionCount = (VariableExpressionExecutor) attributeExpressionExecutors[5];
        }

        if (!(attributeExpressionExecutors[6] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionFt = (VariableExpressionExecutor) attributeExpressionExecutors[6];
        }
        if (!(attributeExpressionExecutors[7] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionRt = (VariableExpressionExecutor) attributeExpressionExecutors[7];
        }
        eventComparator = new EventComparator();
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("Index", Attribute.Type.INT));
        return attributeList;

    }

}
