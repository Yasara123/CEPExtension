package org.wso2.siddhi.extension.twittersentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
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
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

/*
 #TweetReader:getPartyNew2(from_user,5)
 @Param: UserScreenName and Executor Schedule Time
 @Return:Highest Percentage Party and That percentage
 */
public class UserParty extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserParty.class);
    private ComplexEventChunk<StreamEvent> returnEventChunk;
    private VariableExpressionExecutor variableExpressionURLName;
    private static String[] trumpTagArray = { "trump", "donaldtrump", "trump2016", "makeamericagreatagain",
            "realDonaldTrump", "trumpforpresident", "trumppresident2016", "donaldtrumpforpresident",
            "donaldtrumpforpresident2016", "buttrump", "WomenForTrump" };
    private static String[] clintonTagArray = { "hillary2016", "hillaryclinton", "hillaryforpresident2016",
            "imwithher", "hillaryforpresident", "hillary", "HillYes" };
    private static String[] bernieTagArray = { "bernie2016", "feelthebern", "berniesanders", "bernie",
            "bernieforpresident", "bernieorbust", "bernbots", "berniebros", "bernsreturns" };
    private static String[] tedTagArray = { "tedcruz", "cruzcrew", "cruz2016", "makedclisten", "cruzcrew",
            "choosecruz", "tedcruzforpresident", "tedcruz2016", "istandwithtedcruz", "cruztovictory" };
    private static int ScheduleTime = 2;
    private static int corePoolSize = 8;
    private static int maxPoolSize = 400;
    private static long keepAliveTime = 20000;
    private static int jobQueueSize = 10000;
    private static ExecutorService threadExecutor;
    private ThreadFactory threadFactory;
    private static Map<String, String> currentUserChunk;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static BlockingQueue<Runnable> BlockingQ;

    @Override
    public void start() {
        threadFactory = new ThreadFactoryBuilder().setNameFormat("GetParty-%d").build();
        BlockingQ = new LinkedBlockingQueue<Runnable>(jobQueueSize);
        threadExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                BlockingQ, threadFactory);
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                writeDB();
            }
        }, 0, ScheduleTime, HOURS);

    }

    public void writeDB() {
        if (currentUserChunk != null) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date currentDate = new Date();
            String strDate = simpleDateFormat.format(currentDate);
            Set<String> currentKeys = currentUserChunk.keySet();
            java.sql.Connection connection = null;
            java.sql.Statement statement = null;
            try {
                connection = getConnection();
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                for (int i = 0; i < currentUserChunk.size(); i++) {
                    String query = "select * from tweep where name =?";
                    java.sql.PreparedStatement preStatement = connection.prepareStatement(query);
                    preStatement.setString(1, currentKeys.toArray()[i].toString());
                    java.sql.ResultSet resultSet = preStatement.executeQuery();
                    if (!resultSet.next() && !"T".equals(currentUserChunk.get(currentKeys.toArray()[i].toString()))) {
                        String sql = "INSERT INTO tweep VALUES ('" + currentKeys.toArray()[i].toString() + "','"
                                + strDate + "','" + currentUserChunk.get(currentKeys.toArray()[i].toString()) + "')";
                        statement.addBatch(sql);
                    }
                }
                statement.executeBatch();
                connection.commit();
                LOGGER.info("Tweep hashmap finished write to database. ");
            } catch (SQLException e) {
                LOGGER.error("SQL error write to DB ", e);
            } catch (Exception e) {
                LOGGER.error("IO error Enter DB ", e);
            } finally {
                try {
                    if (connection != null) {
                        connection.setAutoCommit(true);
                        statement.close();
                        connection.close();
                    }
                } catch (SQLException e) {
                    LOGGER.error("SQL error when closing DB connection ", e);
                }
            }
        }
        currentUserChunk = new ConcurrentHashMap<String, String>();
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
        threadExecutor.shutdownNow();
        BlockingQ.clear();

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
        returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            if (currentUserChunk.containsKey((String) variableExpressionURLName.execute(streamEvent))) {
                if ("T".equals(currentUserChunk.get((String) variableExpressionURLName.execute(streamEvent)))) {
                    complexEventPopulater.populateComplexEvent(streamEvent, new Object[] { currentUserChunk
                            .get((String) variableExpressionURLName.execute(streamEvent)) });
                    returnEventChunk.add(streamEvent);
                }
            } else {
                currentUserChunk.put((String) variableExpressionURLName.execute(streamEvent), "T");
                threadExecutor.submit(new UserHistoricalTweetAnalysis(streamEvent, complexEventPopulater));
            }
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

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (!(attributeExpressionExecutors.length == 2)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionURLName = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            ScheduleTime = (Integer) attributeExpressionExecutors[1].execute(null);
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("TopName", Attribute.Type.STRING));
        return attributeList;

    }

    public java.sql.Connection getConnection() {
        try {
            String connectionURL = "jdbc:mysql://localhost:3306/ElectionCEP";
            java.sql.Connection connection = null;
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            connection = (java.sql.Connection) DriverManager.getConnection(connectionURL, "root", "mLsxACaH4GECQ");
            return connection;
        } catch (SQLException e) {
            LOGGER.error("SQL error get DB connection function", e);
        } catch (Exception e) {
            LOGGER.error("IO error get DB connection function", e);
        }
        return null;
    }

    class UserHistoricalTweetAnalysis implements Runnable {
        String userName;
        StreamEvent event;
        ComplexEventPopulater complexEventPopulater;

        public UserHistoricalTweetAnalysis(StreamEvent event, ComplexEventPopulater complexEventPopulater) {
            this.userName = (String) variableExpressionURLName.execute(event);
            this.event = event;
            this.complexEventPopulater = complexEventPopulater;
        }

        @Override
        public void run() {
            String tweetChunk = "";
            double trumpCount = 0, bernieCount = 0, clintonCount = 0, tedCount = 0;
            String url = "https://twitter.com/" + userName;
            Document doc;
            int maxeve = 0;
            try {
                Connection con = Jsoup
                        .connect(url)
                        .userAgent(
                                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                        .ignoreContentType(true).ignoreHttpErrors(true).timeout(10000);
                doc = con.get();
                Elements paragraphs = doc.select("b");
                for (Element p : paragraphs) {
                    tweetChunk = tweetChunk.concat(p.text().concat(" "));
                    if (maxeve > 50) {
                        break;
                    }
                    maxeve++;
                }

                for (String tag : trumpTagArray) {
                    trumpCount = trumpCount + StringUtils.countMatches(tweetChunk.toLowerCase(), tag.toLowerCase());
                }
                for (String tag : bernieTagArray) {
                    bernieCount = bernieCount + StringUtils.countMatches(tweetChunk.toLowerCase(), tag.toLowerCase());
                }
                for (String tag : clintonTagArray) {
                    clintonCount = clintonCount + StringUtils.countMatches(tweetChunk.toLowerCase(), tag.toLowerCase());
                }
                for (String tag : tedTagArray) {
                    tedCount = tedCount + StringUtils.countMatches(tweetChunk.toLowerCase(), tag.toLowerCase());
                }

                double sum = trumpCount + bernieCount + clintonCount + tedCount;

                if (sum != 0) {
                    List<EletionCandidate> list = new ArrayList<EletionCandidate>();
                    // todo:check tree list tree map
                    list.add(new EletionCandidate("CLINTON", (double) ((clintonCount / sum) * 100.00)));
                    list.add(new EletionCandidate("TRUMP", (double) ((trumpCount / sum) * 100.00)));
                    list.add(new EletionCandidate("BERNIE", (double) ((bernieCount / sum) * 100.00)));
                    list.add(new EletionCandidate("CRUZ", (double) ((tedCount / sum) * 100.00)));
                    Collections.sort(list, new EletionCandidate());
                    if (currentUserChunk.containsKey((String) variableExpressionURLName.execute(event))
                            && "T".equals(currentUserChunk.get((String) variableExpressionURLName.execute(event)))) {
                        ((ConcurrentHashMap<String, String>) currentUserChunk).replace(
                                (String) variableExpressionURLName.execute(event), new String(list.get(0)
                                        .getCandidateName()));
                    }
                    complexEventPopulater.populateComplexEvent(event, new Object[] { list.get(0).getCandidateName(),
                            list.get(0).getCandidateRank() });
                    addEventChunk(event);
                }

            } catch (IOException e) {

                LOGGER.error("Error Connecting Twitter API  ", e);
            }

        }

    }

}

class EletionCandidate implements Comparator<EletionCandidate>, Comparable<EletionCandidate> {
    private String name;
    private double rank;

    EletionCandidate() {
    }

    EletionCandidate(String n, double a) {
        name = n;
        rank = a;
    }

    public String getCandidateName() {
        return name;
    }

    public double getCandidateRank() {
        return rank;
    }

    @Override
    public int compareTo(EletionCandidate d) {
        return (this.name).compareTo(d.name);
    }

    @Override
    public boolean equals(Object obj) {
        return true;

    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int compare(EletionCandidate d, EletionCandidate d1) {
        return (int) (d1.rank - d.rank);
    }
}
