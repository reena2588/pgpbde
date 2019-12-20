package com.upgrad.storm.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class HashTagCountTopology {

	private static final String CONFIG_FILE                  = "config.properties";
    private static final String TOPOLOGY_NAME                = "HashTag-Count-Topology";
	private static final String TWEET_STREAM_SPOUT_ID        = "TweetStreamSpout";
    private static final String HASHTAG_COUNT_BOLT_ID        = "HashTagCountBolt";
    private static final String HASHTAG_COUNT_REPORT_BOLT_ID = "HashTagCountReportBolt";

    private static HashMap<String,String> twitterConfig      = new HashMap<>();
    private static HashMap<String,String> dbConfig           = new HashMap<>();

    
    private static void readConfig() {
    	
    	try (InputStream input = new FileInputStream(CONFIG_FILE)) {

            Properties prop = new Properties();

            prop.load(input);

            twitterConfig.put("apiKey", prop.getProperty("twitter.apiKey"));
            twitterConfig.put("apiSecretKey", prop.getProperty("twitter.apiSecretKey"));
            twitterConfig.put("accessToken", prop.getProperty("twitter.accessToken"));
            twitterConfig.put("accessTokenSecret", prop.getProperty("twitter.accessTokenSecret"));
            
            dbConfig.put("username", prop.getProperty("db.username"));
            dbConfig.put("password", prop.getProperty("db.password"));
            dbConfig.put("jdbcDriverClass", prop.getProperty("db.jdbcDriverClass"));
            dbConfig.put("jdbcConnectionUrl", prop.getProperty("db.jdbcConnectionUrl"));
            dbConfig.put("deleteQuery", prop.getProperty("db.deleteQuery"));
            dbConfig.put("findQuery", prop.getProperty("db.findQuery"));
            dbConfig.put("insertQuery", prop.getProperty("db.insertQuery"));
            dbConfig.put("updateQuery", prop.getProperty("db.updateQuery"));            
            
        } catch (IOException e) {
        	
            e.printStackTrace();
        
        }
    	
    }

    
    public static void main(String[] args) throws Exception {

    	readConfig();
        
        TweetStreamSpout spout            = new TweetStreamSpout(twitterConfig);
        HashTagCountBolt countBolt        = new HashTagCountBolt();
        HashTagCountReportBolt reportBolt = new HashTagCountReportBolt(dbConfig);
          
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TWEET_STREAM_SPOUT_ID, spout);
        
        //The shuffleGrouping method is used for evenly distributing the tuples to the HashTagCountBolt instances.
        builder.setBolt(HASHTAG_COUNT_BOLT_ID, countBolt, 3).shuffleGrouping(TWEET_STREAM_SPOUT_ID); 
        
        //The fieldsGrouping method is used for distributing tuples with same values to a task of the instances of HashTagCountReportBolt.
        builder.setBolt(HASHTAG_COUNT_REPORT_BOLT_ID, reportBolt).globalGrouping(HASHTAG_COUNT_BOLT_ID); 
        
        Config config = new Config();
        
        // The default time-out of 30 seconds is changed to 5 seconds. 
        // This is to create a possibility of tuple processing failure caused due to time-out.
        config.setMessageTimeoutSecs(5);        
                
        if (args != null && args.length > 0) {
        	
            config.setNumWorkers(3);
            
            // The number of Acker Bolt instances (threads) per worker is increased to 2 to facilitate better parallelism.
            config.setNumAckers(2); 
            
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        
        }
        else {
  			
        	LocalCluster cluster = new LocalCluster();
        	cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        	Thread.sleep(90000);
        	cluster.shutdown();
        
        }

    }
    
}