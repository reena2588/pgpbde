package com.upgrad.storm.twitter;

import java.util.Map;
import java.sql.*; 
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class HashTagCountReportBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Connection connection;
    private Statement statement;
    private ResultSet resultset;
    
    private String username            = "";
    private String password            = "";
    private String jdbcDriverClass     = "";
    private String jdbcConnectionUrl   = "";
    private String deleteQuery         = "";
    private String findQuery           = "";
    private String insertQuery         = "";
    private String updateQuery         = "";

    
    public HashTagCountReportBolt(Map<String, String> dbConfig) {
    	
    	this.username          = dbConfig.get("username");
    	this.password          = dbConfig.get("password");
    	this.jdbcDriverClass   = dbConfig.get("jdbcDriverClass");
    	this.jdbcConnectionUrl = dbConfig.get("jdbcConnectionUrl");
    	
    	this.deleteQuery       = dbConfig.get("deleteQuery");
    	this.findQuery         = dbConfig.get("findQuery");
    	this.insertQuery       = dbConfig.get("insertQuery");
    	this.updateQuery       = dbConfig.get("updateQuery");
    	    
    }
    
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        
    	this.collector = collector;
 
        try {
        	
        	Class.forName(jdbcDriverClass);
        	connection  = DriverManager.getConnection(jdbcConnectionUrl, username, password);  
  		    statement   = connection.createStatement(); 
  		    
  			statement.execute(deleteQuery);
        
        } catch (Exception e) {
  		
        	e.printStackTrace();
        
        }  
        
    }
    
    
	@Override
	public void execute(Tuple tuple) {
    	
		String localFindQuery;
		String localInsertQuery;
		String localUpdateQuery;
		
		try {
        
			String hashtag = tuple.getStringByField("hashtag");
	        	        
	        localFindQuery = findQuery.replace("<1>", hashtag);
	               
	        resultset = statement.executeQuery(localFindQuery);
	        
	        resultset.next();
	        
	        if (resultset.getInt(1) == 0) {
	        	
	        	localInsertQuery = insertQuery.replace("<1>", hashtag);
	        	
	        	statement.executeUpdate(localInsertQuery);
	        	
	        }else {
	        
	        	localUpdateQuery = updateQuery.replace("<1>", hashtag);
	        	
	        	statement.executeUpdate(localUpdateQuery);
	        	
	        }
	        
	        // On successful completion of the tuple processing, the ack() method is called.
            this.collector.ack(tuple); 
            	        
		} catch (Exception e) {
			
			// In case of an erroneous situation or exception, the tuple processing is considered failed and the fail() method is called.
			this.collector.fail(tuple); 
			
			e.printStackTrace();
		
		}
    
	}

	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    	// This bolt does not emit anything as this is the last bolt in the topology.
    
    }

    
    @Override
    public void cleanup() {    
        
        try {
        	
        	resultset.close();
        	statement.close();
			connection.close();
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		
		}
        
    }

}
