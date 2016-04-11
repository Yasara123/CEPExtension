package org.wso2.siddhi.extension.customwin;

import static java.util.concurrent.TimeUnit.HOURS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

public class TimeBatchUniqueProcessor extends StreamProcessor {
    private VariableExpressionExecutor userName;
    private VariableExpressionExecutor userParty;
    private Map<String, StreamEvent> usersMap;
    private int trumpCount, bernieCount, clintonCount, cruzCount;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                usersMap = new ConcurrentHashMap<String, StreamEvent>();
                trumpCount = 0;
                bernieCount = 0;
                clintonCount = 0;
                cruzCount = 0;
            }

        }, 0, 24, HOURS);
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
    }

    @Override
    public Object[] currentState() {
        return new Object[] { trumpCount, bernieCount, clintonCount, cruzCount };
    }

    @Override
    public void restoreState(Object[] state) {
     // No need to maintain a state
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        StreamEvent streamEvent = streamEventChunk.getFirst();
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            StreamEvent oldEvent = usersMap.put((String) userName.execute(streamEvent), streamEvent);
            if (oldEvent == null) {
                String party = (String) userParty.execute(streamEvent);
                if ("TRUMP".equals(party)) {
                    trumpCount = trumpCount + 1;
                } else if ("CLINTON".equals(party)) {
                    clintonCount = clintonCount + 1;
                } else if ("BERNIE".equals(party)) {
                    bernieCount = bernieCount + 1;
                } else if ("CRUZ".equals(party)) {
                    cruzCount = cruzCount + 1;
                }
                complexEventPopulater.populateComplexEvent(streamEvent, new Object[] { trumpCount, clintonCount,
                        cruzCount, bernieCount });
                returnEventChunk.add(streamEvent);
            }
        }
        nextProcessor.process(returnEventChunk);

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 2)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            userName = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            userParty = (VariableExpressionExecutor) attributeExpressionExecutors[1];
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("TRUMP", Attribute.Type.INT));
        attributeList.add(new Attribute("CLINTON", Attribute.Type.INT));
        attributeList.add(new Attribute("CRUZ", Attribute.Type.INT));
        attributeList.add(new Attribute("BERNIE", Attribute.Type.INT));
        return attributeList;
    }

}
