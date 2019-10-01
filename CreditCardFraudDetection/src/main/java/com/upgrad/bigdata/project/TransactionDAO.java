package com.upgrad.bigdata.project;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TransactionDAO implements Serializable{
	
	/* The class TransactionDAO is an implementation of a DAO (Data Access Object). This class contains methods for connecting to the NOSQL (HBase) database and other 
	 * DML (Data Manipulation Language) methods for reading and writing data to the database. 
	 * * */
	
	private static final long serialVersionUID = 1L;
	
	/* This variable stores the HBase connection */
	static Admin hbaseAdmin = null;
	
	/* This variable stores the HBase DB connection parameters */
	static HashMap<String,String> configDB = null;
	
	static final String LOOKUP_TABLE            = "look_up";
	static final String CARD_TRANSACTIONS_TABLE = "card_transactions";
	static final String COLUMN_FAMILY_NAME      = "cf1";
	
	static final String CARD_ID                 = "card_id";
	static final String MEMBER_ID               = "member_id";
	static final String AMOUNT                  = "amount";
	static final String POST_CODE               = "postcode";
	static final String POS_ID                  = "pos_id";
	static final String TRANSACTION_DATE        = "transaction_dt";
	static final String STATUS                  = "status";
	static final String SCORE                   = "score";
	static final String UCL                     = "UCL";
	
	static final String HOST                    = "HOST";
	static final String HBASE_MASTER_PORT       = "HBASE_MASTER_PORT";
	static final String CLIENT_PORT             = "CLIENT_PORT";
	static final String ZOOKEEPER_ZNODE_PARENT  = "/hbase";
	static final int TIME_OUT                   = 1200;
	
	/* This method initializes the configDB HashMap which stores the HBase DB connection parameters.
	 * This method is called from the main class by passing a HashMap created with DB connection parameters received from the command line arguments.
	 * */
	public static void initializeTransactionDAO(Map<String, String> config){
				
		try{
	        
			configDB = new HashMap<>();
			
			configDB.put(HOST, config.get(HOST));
	        configDB.put(HBASE_MASTER_PORT, config.get(HBASE_MASTER_PORT));
	        configDB.put(CLIENT_PORT, config.get(CLIENT_PORT));
			
		}catch(Exception e){
		
			e.printStackTrace();
			
		}
		
	}
	
	/* This method creates and initializes the hbaseAdmin object of type Admin. 
	 * This is a connection object used for connecting to the HBase database.
	 * It uses the DB connection parameters stored in conigDB HashMap to create and establish connection to the database.
	 * */
	public static Admin getHbaseAdmin() throws IOException {
		
		Connection con = null;
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
	
		conf.setInt("timeout", TIME_OUT);
		conf.set("hbase.master", configDB.get(HOST)+":"+configDB.get(HBASE_MASTER_PORT));
		conf.set("hbase.zookeeper.quorum", configDB.get(HOST));
		conf.set("hbase.zookeeper.property.clientPort", configDB.get(CLIENT_PORT));
		conf.set("zookeeper.znode.parent", ZOOKEEPER_ZNODE_PARENT);
	
		try {
	
		    con = ConnectionFactory.createConnection(conf);
			
			if (hbaseAdmin == null){
				
				hbaseAdmin = con.getAdmin();
				
			}
	
			return hbaseAdmin;
	
		} catch (Exception e) {
			
			e.printStackTrace();
			
		}finally{
			
			if (con!= null){
				
				con.close();
			
			}
			
		}
		
		return hbaseAdmin;
	}
	
	/* For a particular card_id, this method retrieves the Member Score from the look_up table available in HBase */
	@SuppressWarnings("deprecation")
	public static Integer getScore(TransactionPOJO transaction) throws IOException {
		
		HTable table = null;
		
		try {

			Get g = new Get(Bytes.toBytes(transaction.getCardID()));
		
			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}
			
			if (hbaseAdmin != null){	
				
				Configuration conf = hbaseAdmin.getConfiguration();
			
				table = new HTable(conf, LOOKUP_TABLE);
			
				Result result = table.get(g);
			
				byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(SCORE));
			
				if (value != null) {
				
					return Integer.parseInt(Bytes.toString(value));
			
				}
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
			
		}
		
		return null;
		
	}
	
	/* For a particular card_id, this method retrieves the UCL from the look_up table available in HBase */
	@SuppressWarnings("deprecation")
	public static Double getUCL(TransactionPOJO transaction) throws IOException {
				
		HTable table = null;
		
		try {
			
			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}

			if (hbaseAdmin != null){	
				
				Get g = new Get(Bytes.toBytes(transaction.getCardID()));

				Configuration conf = hbaseAdmin.getConfiguration();
				
				table = new HTable(conf, LOOKUP_TABLE);
			
				Result result = table.get(g);
			
				byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(UCL));
			
				if (value != null) {
				
					return Double.parseDouble(Bytes.toString(value));
			
				}
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
		}
		
		return null;
	}
	
	/* For a particular card_id, this method retrieves the Post Code of the last transaction from the look_up table available in HBase */
	@SuppressWarnings("deprecation")
	public static Integer getPostCode(TransactionPOJO transaction) throws IOException {
				
		HTable table = null;
		
		try {

			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}

			if (hbaseAdmin != null){	

				Get g = new Get(Bytes.toBytes(transaction.getCardID()));
				
				Configuration conf = hbaseAdmin.getConfiguration();
			
				table = new HTable(conf, LOOKUP_TABLE);
			
				Result result = table.get(g);
			
				byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(POST_CODE));
			
				if (value != null) {
				
					return Integer.parseInt(Bytes.toString(value));
			
				}
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
			
		}
		
		return null;
	
	}
	
	/* For a particular card_id, this method retrieves the Last Transaction Date from the look_up table available in HBase */
	@SuppressWarnings("deprecation")
	public static String getTransactionDate(TransactionPOJO transaction) throws IOException {
				
		HTable table = null;
		
		try {
			
			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}

			if (hbaseAdmin != null){	

				Get g = new Get(Bytes.toBytes(transaction.getCardID()));
				
				Configuration conf = hbaseAdmin.getConfiguration();
			
				table = new HTable(conf, LOOKUP_TABLE);
			
				Result result = table.get(g);
			
				byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(TRANSACTION_DATE));
			
				if (value != null) {  
				
					return Bytes.toString(value);
			
				}
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
		}
		
		return null;
	}

	/* Once a transaction is categorized as GENUINE or FRAUD, this method inserts the transaction details for a particular transaction
	 * in the card_transactions table in HBase.
	 *  */
	@SuppressWarnings("deprecation")
	public static void insertTransaction(TransactionPOJO transaction) throws IOException {
				
		HTable table = null;
		
		String rowID = null;
		
		try {

			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}
	
			if (hbaseAdmin != null){
			
				Configuration conf = hbaseAdmin.getConfiguration();
				
				table = new HTable(conf, CARD_TRANSACTIONS_TABLE);
			
				rowID = transaction.getCardID()+"~"+transaction.getTransactionDate()+"~"+transaction.getAmount();
			
				Put data = new Put(Bytes.toBytes(rowID));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(CARD_ID), Bytes.toBytes(transaction.getCardID()));

				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(MEMBER_ID), Bytes.toBytes(transaction.getMemberID()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(AMOUNT), Bytes.toBytes(transaction.getAmount()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(POST_CODE), Bytes.toBytes(transaction.getPostCode()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(POS_ID), Bytes.toBytes(transaction.getPosID()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(TRANSACTION_DATE), Bytes.toBytes(transaction.getTransactionDate()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(STATUS), Bytes.toBytes(transaction.getStatus()));
			
				table.put(data);
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
			
		}
		
	}
	
	/* If a transaction is categorized as GENUINE, this method updates the new post code and transaction date in the look_up table for a particular member. 
	 * */
	@SuppressWarnings("deprecation")
	public static void updateLookUp(TransactionPOJO transaction) throws IOException {
				
		HTable table = null;
		
		String rowID = null;
		
		try {

			if (hbaseAdmin == null){
				
				hbaseAdmin = getHbaseAdmin();	
			
			}

			if (hbaseAdmin != null){	
			
				Configuration conf = hbaseAdmin.getConfiguration();
				
				table = new HTable(conf, LOOKUP_TABLE);
			
				rowID = transaction.getCardID();
			
				Put data = new Put(Bytes.toBytes(rowID));
						
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(POST_CODE), Bytes.toBytes(transaction.getPostCode()));
			
				data.add(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(TRANSACTION_DATE), Bytes.toBytes(transaction.getTransactionDate()));
			
				table.put(data);
				
			}
		
		} catch (Exception e) {
			
			e.printStackTrace();
		
		} finally {
		
			try {
			
				if (table != null){
					
					table.close();
			
				}
				
				if (hbaseAdmin != null){
					
					hbaseAdmin.close();
					
				}
			
			} catch (Exception e) {
				
				e.printStackTrace();
			
			}
		}
	}

}