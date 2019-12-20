package com.upgrad.storm.twitter;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetStreamSpout extends BaseRichSpout {
	
	/* A ConcurrentHashMap (thread-safe) is defined to store the tuples and their message IDs which will be used for anchoring and 
	 * for ack() and fail() methods.
    */
    private ConcurrentHashMap <UUID, Values> pendingMsgQueue; 
    
	private LinkedBlockingQueue<Status> queue = null;
	
	private SpoutOutputCollector collector;
	
	private String apiKey;
	private String apiSecretKey;
	private String accessToken;
	private String accessTokenSecret;
	
	private TwitterStream twitterStream;
	
	
    public TweetStreamSpout(Map<String, String> twitterConfig) {
    
    	this.apiKey            = twitterConfig.get("apiKey");
    	this.apiSecretKey      = twitterConfig.get("apiSecretKey");
    	this.accessToken       = twitterConfig.get("accessToken");
    	this.accessTokenSecret = twitterConfig.get("accessTokenSecret");
    	    
    }

    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
        declarer.declare(new Fields("hashtag"));
        
    }

    
    @Override
    public Map<String, Object> getComponentConfiguration() {

       Config config = new Config();
       config.setMaxTaskParallelism(1);
       
       return config;
    
    }
    
    
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
    	
        this.collector       = collector;
        
        // The ConcurrentHashMap is initialized. The UUID stores the unique msgID and the Values stores the tuples to be emitted.
        this.pendingMsgQueue = new ConcurrentHashMap<>(); 
    
        queue = new LinkedBlockingQueue<>(1000);
        
        StatusListener listener = new StatusListener() {
            
        	@Override
            public void onStatus(Status status) {
        		queue.offer(status);
            }
					
            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            	//Do Nothing
            }
					
            @Override
            public void onTrackLimitationNotice(int i) {
            	//Do Nothing
            }
					
            @Override
            public void onScrubGeo(long l, long l1) {
            	//Do Nothing
            }
					
            @Override
            public void onException(Exception ex) {
            	//Do Nothing
            }
					
            @Override
            public void onStallWarning(StallWarning arg0) {
            	//Do Nothing
            }
            
         };
				
         ConfigurationBuilder builder = new ConfigurationBuilder();
				
         builder.setDebugEnabled(true)
                .setOAuthConsumerKey(apiKey)
                .setOAuthConsumerSecret(apiSecretKey)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
					
         twitterStream = new TwitterStreamFactory(builder.build()).getInstance();
         twitterStream.addListener(listener);
	        
         twitterStream.sample();
          
    }

    
    public void nextTuple() {
    	
    	UUID msgId;
    	
    	Status tweet = queue.poll();
    	
    	try {
  
        	if (tweet == null) {

        		Utils.sleep(50);
             
        	} else {
        		
        		if (tweet.getLang().equals("en")) {
        			
        			for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
        		        		    	
        				msgId = UUID.randomUUID();
            			
        				Values tupleValue = new Values(hashtag.getText());

        				// For each tuple, a unique msgID is generated and stored in the ConcurrentHashMap along with the tuple.
        				this.pendingMsgQueue.put(msgId, tupleValue);
        		    	
        				// The tuple along with the msgID is emitted to Bolt for processing. This is called Anchoring.
        				this.collector.emit(tupleValue, msgId);
        			
        			}
        		
        		}
        
        	}
    	
    	} catch (Exception e) {
    		
    		e.printStackTrace();
    	
    	}
    
    }
    
    
    @Override
    public void close() {
       
    	twitterStream.shutdown();
    
    }
    
    
    @Override
    public void ack(Object msgID) {
    	
       	System.out.println("Tuple Processed Successfully With Message ID: "+msgID.toString());

    	/* The tuple with a particular message ID is removed from the ConcurrentHashMap as this method is called 
    	 * when the tuple is completely processed and hence is not required to be stored in the pending message queue ConcurrentHashMap.
       	 */
       	this.pendingMsgQueue.remove(msgID);

    }
    
    
    @Override
    public void fail(Object msgID) {
    	
    	System.out.println("Tuple Processing Failed For Tuple With Message ID: "+msgID.toString());

    	System.out.println("Replay For Tuple With Message ID: "+msgID.toString());

    	/* The tuple failed during processing in one of the bolts is replayed using the emit() method. 
    	 * The tuple stored along with the msgID is retrieved from the ConcurrentHashMap using msgID as Key.
    	 */
    	this.collector.emit(this.pendingMsgQueue.get(msgID), msgID);
    		
    }
    
}