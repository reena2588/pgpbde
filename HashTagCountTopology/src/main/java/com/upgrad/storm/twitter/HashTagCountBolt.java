package com.upgrad.storm.twitter;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HashTagCountBolt extends BaseRichBolt {
    
	private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        
    	this.collector = collector;
        this.counts    = new HashMap<>();
    
    }
    
    
	@Override
	public void execute(Tuple tuple) {	

		String hashtag = tuple.getStringByField("hashtag");   
        Long count     = this.counts.get(hashtag);
        
        try {
        
	        if(count == null) {
	        
	        	count = 0L;
	        
	        }
	        
	        count++;
	        
	        this.counts.put(hashtag, count);
	     
	        this.collector.emit(new Values(hashtag, count));

	        // On successful completion of the tuple processing, the ack() method is called.
	        this.collector.ack(tuple); 
	        	        
        }catch (Exception e) {
        	
        	// In case of an erroneous situation or exception, the tuple processing is considered failed and the fail() method is called.
        	this.collector.fail(tuple); 
        	
        	e.printStackTrace();
        
        }
		
	}


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    	declarer.declare(new Fields("hashtag", "count"));
    
    }
    
    
    @Override
    public void cleanup() {
    	
    	// Do Nothing
       
    }
    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
       
    	return null;
    
    }

}