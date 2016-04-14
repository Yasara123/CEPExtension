package org.wso2.siddhi.extension.clearsentiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

/*#Sentiment:getRate(text)
 * Sample Query:
 * from inputStream#Sentiment:getRate(text)
 * select attribute1, attribute2
 * insert into outputStream;
 */
public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);
    private String contentText;
    private static String OP = "COMMON";
    private String[] positiveWordBucket;
    private String[] negativeWordBucket;
    private String[] wordBucket;

    @Override
    public Type getReturnType() {
        return Type.INT;
    }

    @Override
    public void start() {
        // No need to maintain a state
    }

    @Override
    public void stop() {
        // No need to maintain a state
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
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to ClearSentiment:TrackWord() function, "
                            + "required 2, but found " + attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            OP = (String) attributeExpressionExecutors[1].execute(null);
        }
        Attribute.Type attributeType1 = attributeExpressionExecutors[0].getReturnType();
        if (!(attributeType1 == Attribute.Type.STRING)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the argument 1 of ClearSentiment:TrackWord() function");
        }
        if ("COMMON".equals(OP)) {
            positiveWordBucket = getWordsBuckets("/positivewords.txt");
            negativeWordBucket = getWordsBuckets("/negativewords.txt");
        } else if ("AFFIN".equals(OP)) {
            wordBucket = getWordsBuckets("/affinwords.txt");
        } else if ("STANFORD".equals(OP)) {
            //No need any word set
        } else {
            LOGGER.error("Invalid option of sentimentrate.java clas. Option can be only COMMON,AFFIN or STANFORD");
        }

    }

    protected String[] getWordsBuckets(String filename) {
        BufferedReader br = null;
        String[] wordList = null;
        try {
            InputStream in = getClass().getResourceAsStream(filename);
            br = new BufferedReader(new InputStreamReader(in));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            String everything = sb.toString();
            wordList = everything.split(",");
            
        } catch (FileNotFoundException e) {
            LOGGER.error(filename + " file not found error in sentimentRate class " + e);
        } catch (IOException e) {
            LOGGER.error(filename + " IO error in sentimentRate class " + e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                LOGGER.error(filename + " IO error in sentimentRate class when closing buffered reader " + e);
            }
        }
        return wordList;

    }

    @Override
    protected Object execute(Object[] data) {
        int rank = 0;
        contentText = (String) data[0];
        if ("COMMON".equals(OP)) {
            rank = getCommonSentimentRate(contentText);
        } else if ("AFFIN".equals(OP)) {
            rank = getAffinSentimentRate(contentText);
        } else if ("STANFORD".equals(OP)) {
            rank = getStanfordSentimentRate(contentText);
        } else {
            LOGGER.error("Invalid option of sentimentrate.java class. Option can be only COMMON,AFFIN or STANFORD");
        }
        return rank;
    }

    protected int getCommonSentimentRate(String contentText) {
        int positiveRank = 0;
        for (int i = 0; i < positiveWordBucket.length; i++) {
            Matcher m = Pattern.compile("\\b" + positiveWordBucket[i] + "\\b").matcher(contentText);
            while (m.find()) {
                positiveRank++;
            }
        }
        int negativeRank = 0;
        for (int i = 0; i < negativeWordBucket.length; i++) {
            Matcher m = Pattern.compile("\\b" + negativeWordBucket[i] + "\\b").matcher(contentText);
            while (m.find()) {
                negativeRank++;
            }
        }
        return positiveRank - negativeRank;

    }

    protected int getAffinSentimentRate(String contentText) {
        int rank = 0;
        String[] splt;
        int val = 0;
        String word = "null";
        for (int i = 0; i < wordBucket.length; i++) {
            splt = wordBucket[i].split(" ");
            word = splt[0];
            val = Integer.parseInt(splt[splt.length - 1].trim());
            Matcher m = Pattern.compile("\\b" + word + "\\b").matcher(contentText);
            while (m.find()){
                rank = rank + val;
            }
        }
        return rank;
    }

    protected int getStanfordSentimentRate(String sentimentText) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int totRate = 0;
        Annotation annotation = pipeline.process(sentimentText);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentAnnotatedTree.class);
            int score = RNNCoreAnnotations.getPredictedClass(tree);
            totRate = totRate + (score-2);
        }
        return totRate;
    }

    @Override
    protected Object execute(Object data) {
        return data;
        
    }
}
