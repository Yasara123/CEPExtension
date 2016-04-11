package org.wso2.siddhi.extension.clearsentiments;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

/*
 *#Sentiment:trackWord(text,"Trump","Donald","He")
 * Sample Query:
 * from inputStream#Sentiment:trackWord(text,"Trump","Donald","He")
 * select attribute1, attribute2
 * insert into outputStream;
 */

public class CandidateTextLines extends FunctionExecutor {

    @Override
    public Type getReturnType() {
        return Type.STRING;
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
        if (attributeExpressionExecutors.length != 4) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to ClearSentiment:TrackWord() function, "
                            + "required 2, but found " + attributeExpressionExecutors.length);
        }
        Attribute.Type attributeType1 = attributeExpressionExecutors[0].getReturnType();
        if (!(attributeType1 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType2 = attributeExpressionExecutors[1].getReturnType();
        if (!(attributeType2 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType3 = attributeExpressionExecutors[2].getReturnType();
        if (!(attributeType3 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
        Attribute.Type attributeType4 = attributeExpressionExecutors[3].getReturnType();
        if (!(attributeType4 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument of ClearSentiment:TrackWord() function");
        }
    }

    @Override
    protected Object execute(Object[] data) {
        String text = "";
        if (data != null) {
            String[] arr = ((String) data[0]).split("\\.");
            for (String line : arr) {
                if ((line.contains((String) data[1])) || (line.contains((String) data[2]))
                        || (line.contains((String) data[3]))) {
                    text = text.concat(line.concat(" "));
                }
            }
        } else {
            throw new ExecutionPlanRuntimeException("Input to the ClearSentiment:TrackWord() function cannot be null");
        }
        return text;
    }

    @Override
    protected Object execute(Object data) {
        Object arr = new Object[0];
        return arr;// No need to maintain a state.
    }

}
