package com.upgrad.pgpbde.project.map.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** 
 * The <b> SaavnStreamingKey </b> class is used as the Output Key class for the Mapper class <b> SaavnStreamingMapper </b>. 
 * <p>
 * A Map Output Key is constructed using an object of this class.
 * <p>
 * It contains <b> trendingDay </b> and <b> songId </b> as its members and overrides the following methods:
 * <p>
 * readFields()
 * <p>
 * write()
 * <p>
 * hashCode()
 * <p>
 * compareTo()
 */

public class SaavnStreamingKey implements WritableComparable<SaavnStreamingKey> {

	/** The value for the <b> trendingDay </b> member could be any value between 25th December to 31st December (both days inclusive). 
	 *  It is the day on which a particular song is trending.
	 * */
	private Text trendingDay;
	
	/** The value for the <b> songId </b> member is an id which identifies a song. */
	private Text songId;

	public SaavnStreamingKey() {
		
		this.trendingDay = new Text();
        this.songId = new Text();
		
	}

	public SaavnStreamingKey(Text trendingDay, Text songId) {

		this.trendingDay = trendingDay;
		this.songId    = songId;
	
	}
	
	public Text getTrendingDay() {
		
		return trendingDay;
	
	}
	
	public void setStreamDay(Text trendingDay) {
		
		this.trendingDay = trendingDay;
	
	}

	public Text getSongID() {
		
		return songId;
	
	}
	
	public void setSongID(Text songId) {
		
		this.songId = songId;
	
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		this.trendingDay.readFields(dataInput);
        this.songId.readFields(dataInput);
		 
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
	    
		this.trendingDay.write(dataOutput);
        this.songId.write(dataOutput);
		
	}
	
	@Override
	public int hashCode() {
		
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((trendingDay == null) ? 0 : trendingDay.hashCode());
	    result = prime * result + ((songId == null) ? 0 : songId.hashCode());
	    return result;
	
	}
	
	@Override
	public boolean equals(Object obj) {
		
		return true;
		
	}
	
	public int compareTo(SaavnStreamingKey pop) {
		
        if (pop == null)
            return 0;
        
        int intcnt = trendingDay.compareTo(pop.trendingDay);
        
        if (intcnt != 0) {
            
        	return intcnt;
        
        } else {
            
        	return songId.compareTo(pop.songId);

        }
    }

}