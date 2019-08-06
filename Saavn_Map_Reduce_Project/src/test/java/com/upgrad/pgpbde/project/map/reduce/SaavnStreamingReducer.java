package com.upgrad.pgpbde.project.map.reduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
 * The <b> SaavnStreamingReducer </b> class is an implementation of the <b> Reducer </b> class which is used to aggregate the map output which is partitioned based on <b> trendingDay </b>.
 * <p> 
 * All the reduce outputs i.e. 07 files contains the top 100 trending songs for each day.
 */
public class SaavnStreamingReducer extends Reducer<SaavnStreamingKey, SaavnStreamingValue, Text, NullWritable> {
		
		/** For each trendingDay, the latest instance of a streamed song is stored with its songId and songWeight. */
		HashMap<String, Integer> songStream = new HashMap<>();
		
		/**
		 * An implementation of the <b> reduce() </b> method in the Reducer class <b> SaavnStreamingReducer </b> which aggregates trending songs based on the weight assigned to each song.
		 */
		@Override
		public void reduce(SaavnStreamingKey key, Iterable<SaavnStreamingValue> valueList, Context context) throws IOException, InterruptedException {
			
			int songWeight = 0;
			
			for (SaavnStreamingValue ssr : valueList) {
			
				songWeight = songWeight + ssr.getSongWeight();
			
			}
			
			String songId = ""+key.getSongID();
			
			if (songStream.isEmpty()){
				
				songStream.put(songId, Integer.valueOf(songWeight));
				
			}else{
				
				if (songStream.containsKey(songId)){
					
					Integer currWeight = songStream.get(songId);
					
					if (songWeight >= currWeight){

						songStream.put(songId, Integer.valueOf(songWeight));
							
					}
					
				}else{
					
					songStream.put(songId, Integer.valueOf(songWeight));
				
				}
			
			}		
			
		}
		
		/**
		* The implementation of the <b> cleanup() </b> method called only once for a reducer emits all 100 songs sorted by descending of songWeight for each day from 25th December to 31st December
		*/
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			int top = 0;
			
			Map<String, Integer> map = sortByValues(songStream); 
		    
			Set set                  = map.entrySet();
		    Iterator iterator        = set.iterator();
		    
		    while(iterator.hasNext()) {
		    	
		    		if (top <100) {
		    			
		    			Map.Entry me = (Map.Entry)iterator.next();
		           
		    			Text songId = new Text((String) me.getKey());
		    			
		    			context.write(songId, NullWritable.get());
		    			
		    			top++;
		    		
		    		}else{

		    			break;
		    			
		    		}
		      }
	            
		}
		
		/** 
		 * This method sorts the top 100 trending songs in descending order. 
		*/
		@SuppressWarnings({ "unchecked", "rawtypes" })
		private static HashMap sortByValues(HashMap songStream) {
			
			List list = new LinkedList(songStream.entrySet());
	
		       Collections.sort(list, new Comparator() {
		    	   
		            public int compare(Object o1, Object o2) {
		            	
		               return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
		            
		            }
		       
		       });

		       HashMap sortedHashMap = new LinkedHashMap();
		       
		       for (Iterator it = list.iterator(); it.hasNext();) {
		              
		    	   	  Map.Entry entry = (Map.Entry) it.next();

		    	   	  sortedHashMap.put(entry.getKey(), entry.getValue());
		       
		       } 
		       
		       return sortedHashMap;
		  }
}