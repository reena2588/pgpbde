package com.upgrad.sparkstreaming.project;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import scala.Tuple2;

public class StockDataAnalysis {
	
	static String inputStreamFolderLocation            = "";
	static String outputStreamFolderLocation           = "";			
		
	/* The SparkConf object is created and initialized to null */
	static SparkConf conf                              = null;
	
	/*The JavaStreamingContext is created and initialized to null */
	static JavaStreamingContext jsc                    = null;
	
	private StockDataAnalysis(){
		
		//Does Nothing
		
	}

	public static void simpleMovingAvgClosePrice(String[] args) {
		
		/* This is the APP NAME to be set while initializing the SparkConf object */
		final String APP_NAME 				       = "SparkStreamingStockDataAnalysis1";
		
		/* This is the name of the analysis to be performed on streaming data*/
		final String ANALYSIS_NAME		           = "SimpleMovingAverage-Close-Price";
		
		/* The Seconds variable contains value as 60. This value will be later used to convert seconds into minutes for Batch Interval, Window Duration and Sliding Window Duration. */
		int secs								   = 60;
		
		/* The Batch Interval Is Defined To Be 01 Minute */
		int batchInterval                          = 01;
		
		/* The Window Duration Is Defined To Be 10 Minutes */
		int windowDuration                         = 10;
		
		/* The Sliding Window Duration Is Defined To Be 05 Minutes */
		int slidingWindowDuration                  = 05;

		try {
			
			/* The first command-line value which points to the absolute path on the file system where streaming input data is stored. */ 
			inputStreamFolderLocation  = args[0];
			
			/* The second command-line value which points to absolute path on the file system where output data is used to be stored. */
			outputStreamFolderLocation = args[1]+ANALYSIS_NAME;
			
			/* The SparkConf object is re-initialized to run in local mode. An APPNAME is set for Spark to uniquely distinguish the spark streaming context for this program execution. */
			conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME);
		
			/* The JavaStreamingContext is re-initialized. The SparkConf object is passed along with Batch Interval (in seconds) */
			jsc = new JavaStreamingContext(conf, Durations.seconds(batchInterval*(long)secs));
		
			/* Introduced Logger to reduce Spark Logging on console */
			Logger.getRootLogger().setLevel(Level.ERROR);
		
			/* The textFileStream() method is used to fetch streaming data from the input location and create DStream of type JavaDStream<String>. The JSON data will be stored as String. */
			JavaDStream<String> jsonData = jsc.textFileStream(inputStreamFolderLocation).cache();
					
			/* The FlatMapFunction jsonExtractor  is used for a flatMap() transformation. 
			 * This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The class Stock is an implementation of a serializable POJO with composition of Price data.
			 * Logic:
			 * 1. The FlatMapFunction casts the JSON String into an object of type JSONArray
			 * 2. An ArrayList<Stock> is created and initialized
			 * 3. The JSONArray object is looped through to fetch the individual data elements and store them in an object of Stock type.
			 * 4. The Stock object is added to the Stock ArrayList.
			 * 5. There is an IF-ELSE construct which is used to calculate the total number of periods within a window, which will be used as a denominator for calculating Simple Moving Average. 
			 * 5.a The total number of periods is stored within the Stock class as a Static member variable.
			 * 5.b If the total number of periods is greater than Window Duration then the total number of periods is set to the value of Window Duration i.e. 10.
			 * 5.c Else the total number of periods is incremented by one.
			 * 5.d This logic is coded to make sure that in the first window the total number of periods is 5 as the sliding interval is 5 minutes and it is the first window.
			 * 5.e Later on for all windows, a total number of periods will always be 10 as the Window Duration is 10.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			final FlatMapFunction<String, Stock> jsonExtractor = new FlatMapFunction<String, Stock>() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Stock> call(String istock)  {
					
					Stock stock;
					Price price;
					JSONObject priceData;
					    
					JSONArray jsonStock = (JSONArray) JSONSerializer.toJSON(istock);
				
					if (Stock.getTotalNoPeriod()>windowDuration) {
					
						Stock.setTotalNoPeriod(windowDuration);
					
					}else {
					
						Stock.incrTotalNoPeriod();
					
					}
				    
					ArrayList<Stock> stockArrList= new ArrayList<>(); 
					
					for(int i=0 ; i< jsonStock.size() ;i++) {
						
						stock = new Stock();
						price = new Price();
				    
					    stock.setSymbol((String)jsonStock.getJSONObject(i).get("symbol"));
					    stock.setTimestamp((String)jsonStock.getJSONObject(i).get("timestamp"));
					    priceData = jsonStock.getJSONObject(i).getJSONObject("priceData");
					    	
					    price.setOpen(Double.valueOf((String) priceData.get("open")));
					    price.setHigh(Double.valueOf((String) priceData.get("high")));
					    price.setLow(Double.valueOf((String) priceData.get("low")));
					    price.setClose(Double.valueOf((String) priceData.get("close")));
					    price.setVolume(Integer.valueOf((String) priceData.get("volume")));
					    	
					    stock.setPriceData(price);
					    	
					    stockArrList.add(stock);
		
					}
					
					return stockArrList.iterator();
				
				}
			};
		
			/* The PairFunction stockMapper is used for a mapToPair() transformation.
			 * This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol and Close Price from the Stock POJO and returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			final PairFunction stockMapper = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Object arg0) throws Exception {
				
					Stock st      = (Stock)arg0;
					String symbol = st.getSymbol();
					Double close  = st.getPriceData().getClose();
				
					return new Tuple2<>(symbol, close);
			
				}
		
			};
		
			/* The Function2 stockReducer is used for a reduceByKeyAndWindow() transformation.
			 * The reduceByKeyAndWindow() takes as input the following 03 arguments:
			 * a. Name of the function (of type Function2)
			 * b. Window Duration (in seconds)
			 * c. Sliding Window Duration (in seconds) 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Close Price.
			 * The function aggregates the Close Price of all stocks. 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			final Function2<Double, Double, Double> stockReducer = new Function2<Double, Double, Double>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Double call(Double a, Double b) throws Exception {
	
		          return a + b;
	
				}
				
			};
		
			/* The PairFunction stockAverage is used for a mapToPair() transformation 
			 * This transformation operates on JavaPairDStream<String, Double> which contains the aggregated close price of stocks.
			 * The PairFunction extracts the following data elements from Stock
			 * a. Symbol
			 * b. Close Price
			 * c. Total Number of Period (calculated in the JSON_EXTRACTOR function.
			 * The PairFunction calculates the close price moving average by dividing the aggregated close price by total number of periods.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			final PairFunction stockAverage = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Object arg0) throws Exception {
				
					Tuple2<String, Double> temp = (Tuple2)arg0;
				
					String symbol = temp._1();
					Double avg    = Double.valueOf(temp._2()/Stock.getTotalNoPeriod());
				
					return new Tuple2<>(symbol, avg);
			
				}
		
			};
			
			/* This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			JavaDStream<Stock> stockData = jsonData.flatMap(jsonExtractor);		
			
			/* This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol and Close Price from the Stock POJO and returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			JavaPairDStream<String, Double> stock = stockData.mapToPair(stockMapper);				  
			
			/* This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Close Price.
			 * The function aggregates the Close Price of all stocks. 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			JavaPairDStream<String, Double> stockSum = stock.reduceByKeyAndWindow(stockReducer, Durations.seconds(windowDuration*(long)secs), Durations.seconds(slidingWindowDuration*(long)secs));
			
			/* This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			JavaPairDStream stockAvg = stockSum.mapToPair(stockAverage);
			
			/* The final output i.e. Stock With Simple Moving Average (Symbol, Simple Moving Average For Close Price) is printed on the console. 
			 * This will be printed once for each window 
			 * */
			stockAvg.print();
			
			/* The final output i.e. Stock With Simple Moving Average (Symbol, Simple Moving Average For Close Price) is saved to an output location on the local disk where the program is executed.
			 * The OUTPUT_STREAM_FOLDER_LOCATION points to the path where the output is to be generated.
			 * The output will be generated once for each window 
			 * */
			stockAvg.dstream().repartition(1).saveAsTextFiles(outputStreamFolderLocation, "");
			
			jsc.start();
			
			jsc.awaitTermination();
		
			jsc.close();
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}finally {
			
			try {
				
				if (jsc!=null) {
					
					jsc.close();
			
				}
			}catch(Exception e) {
				
				e.printStackTrace();
			
			}
		}
		
	}
	
	public static void stockWithMaxProfit(String[] args) {
		
		/* This is the APP NAME to be set while initializing the SparkConf object */
		final String APP_NAME 				       = "SparkStreamingStockDataAnalysis2";
		
		/* This is the name of the analysis to be performed on streaming data*/
		final String ANALYSIS_NAME		           = "Stock With Maximum Profit";
		
		/* The Seconds variable contains value as 60. This value will be later used to convert seconds into minutes for Batch Interval, Window Duration and Sliding Window Duration. */
		int secs								   = 60;
		
		/* The Batch Interval Is Defined To Be 01 Minute */
		int batchInterval                          = 01;
		
		/* The Window Duration Is Defined To Be 10 Minutes */
		int windowDuration                         = 10;
		
		/* The Sliding Window Duration Is Defined To Be 05 Minutes */
		int slidingWindowDuration                  = 05;
		
		try {
			
			/* The first command-line value which points to the absolute path on the file system where streaming input data is stored. */
			inputStreamFolderLocation  = args[0];
			
			/* The second command-line value which points to absolute path on the file system where output data is used to be stored. */
			outputStreamFolderLocation = args[1]+ANALYSIS_NAME;
			
			/* The SparkConf object is re-initialized to run in local mode. An APPNAME is set for Spark to uniquely distinguish the spark streaming context for this program execution. */
			conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME);
		
			/* The JavaStreamingContext is re-initialized. The SparkConf object is passed along with Batch Interval (in seconds) */
			jsc = new JavaStreamingContext(conf, Durations.seconds(batchInterval*(long)secs));
			
			/* Introduced Logger to reduce Spark Logging on console */
			Logger.getRootLogger().setLevel(Level.ERROR);
		
			/* The textFileStream() method is used to fetch streaming data from the input location and create DStream of type JavaDStream<String>. The JSON data will be stored as String. */
			JavaDStream<String> jsonData = jsc.textFileStream(inputStreamFolderLocation).cache();
		
			/* The FlatMapFunction jsonExtractor  is used for a flatMap() transformation. 
			 * This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The class Stock is an implementation of a serializable POJO with composition of Price data.
			 * Logic:
			 * 1. The FlatMapFunction casts the JSON String into an object of type JSONArray
			 * 2. An ArrayList<Stock> is created and initialized
			 * 3. The JSONArray object is looped through to fetch the individual data elements and store them in an object of Stock type.
			 * 4. The Stock object is added to the Stock ArrayList.
			 * 5. There is an IF-ELSE construct which is used to calculate the total number of periods within a window, which will be used as a denominator for calculating Open Price Average and Close Price Average. 
			 * 5.a The total number of periods is stored within the Stock class as a Static member variable.
			 * 5.b If the total number of periods is greater than Window Duration then the total number of periods is set to the value of Window Duration i.e. 10.
			 * 5.c Else the total number of periods is incremented by one.
			 * 5.d This logic is coded to make sure that in the first window the total number of periods is 5 as the sliding interval is 5 minutes and it is the first window.
			 * 5.e Later on for all windows, a total number of periods will always be 10 as the Window Duration is 10.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			final FlatMapFunction<String, Stock> jsonExtractor = new FlatMapFunction<String, Stock>() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Stock> call(String istock)  {
					
					Stock stock;
					Price price;
					JSONObject priceData;
					    
					JSONArray jsonStock = (JSONArray) JSONSerializer.toJSON(istock);
				
					if (Stock.getTotalNoPeriod()>windowDuration) {
					
						Stock.setTotalNoPeriod(windowDuration);
					
					}else {
					
						Stock.incrTotalNoPeriod();
					
					}
				    
					ArrayList<Stock> stockArrList= new ArrayList<>(); 
					
					for(int i=0 ; i< jsonStock.size() ;i++) {
						
						stock = new Stock();
						price = new Price();
				    
						stock.setSymbol((String)jsonStock.getJSONObject(i).get("symbol"));
						stock.setTimestamp((String)jsonStock.getJSONObject(i).get("timestamp"));
					    priceData = jsonStock.getJSONObject(i).getJSONObject("priceData");
					    	
					    price.setOpen(Double.valueOf((String) priceData.get("open")));
					    price.setHigh(Double.valueOf((String) priceData.get("high")));
					    price.setLow(Double.valueOf((String) priceData.get("low")));
					    price.setClose(Double.valueOf((String) priceData.get("close")));
					    price.setVolume(Integer.valueOf((String) priceData.get("volume")));
					    	
					    stock.setPriceData(price);
					    	
					    stockArrList.add(stock);
		
					}
					
					return stockArrList.iterator();
				
				}
			};
		
			/* The PairFunction stockMapper is used for a mapToPair() transformation.
			 * This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol, Open Price and Close Price from the Stock POJO.
			 * It creates a ArrayList<Double> and adds both the Open Price and Close Price for a Stock in the ArrayList.
			 * A Tuple2<String, List<Double>> is returned. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, List<Double>>.
			 * */
			final PairFunction stockMapper = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, List<Double>> call(Object arg0) throws Exception {
				
					Stock st           = (Stock)arg0;
					List<Double> lst   = new ArrayList<>();
					
					String symbol      = st.getSymbol();
					Double open        = st.getPriceData().getOpen();
					Double close       = st.getPriceData().getClose();
										
					lst.add(0, open);
					lst.add(1, close);
					
					return new Tuple2<>(symbol, lst);
			
				}
		
			};
		
			/* The Function2 stockReducer is used for a reduceByKeyAndWindow() transformation.
			 * The reduceByKeyAndWindow() takes as input the following 03 arguments:
			 * a. Name of the function (of type Function2)
			 * b. Window Duration (in seconds)
			 * c. Sliding Window Duration (in seconds) 
			 * This transformation operates on JavaPairDStream<String, List<Double>> which contains Stock Symbol, Open Price and Close Price.
			 * The function aggregates the Open Prices and Close Prices of all stocks. 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * It creates a ArrayList<Double> and adds both the aggregated Open Prices and aggregated Close Prices for a Stock in the ArrayList.
			 * A Tuple2<String, List<Double>> is returned.
			 * The transformed RDDs are stored in a JavaPairDStream<String, List<Double>>
			 * */
			final Function2<List<Double>, List<Double>, List<Double>> stockReducer = new Function2<List<Double>, List<Double>, List<Double>>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public List<Double> call(List<Double> arg0, List<Double> arg1) throws Exception {
					
					List<Double> temp = new ArrayList<>();
					
					Double open  = arg0.get(0)+arg1.get(0);
					Double close = arg0.get(1)+arg1.get(1);
					
					temp.add(0, open);
					temp.add(1, close);
		          
					return temp;
	
				}
				
			};
		
			/* The PairFunction stockProfitList is used for a mapToPair() transformation 
			 * This transformation operates on JavaPairDStream<String, List<Double>> which contains the aggregated open price and close price of stocks.
			 * The PairFunction extracts the following data elements from Stock
			 * a. Symbol
			 * b. Open Price
			 * c, Close Price
			 * d. Total Number of Period (calculated in the JSON_EXTRACTOR function.
			 * The PairFunction calculates 
			 * a. The open price average by dividing the aggregated open price by total number of periods
			 * b. The close price average by dividing the aggregated close price by total number of periods.
			 * c. The profit by subtracting the open price average from the close price average.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Symbol and Profit which is calculated.
			 * */
			final PairFunction stockProfitList = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Object arg0) throws Exception {
				
					Tuple2<String, List<Double>> temp = (Tuple2)arg0;
				
					String symbol             = temp._1();
					Double openPriceAvg       = Double.valueOf(temp._2().get(0)/Stock.getTotalNoPeriod());
					Double closePriceAvg      = Double.valueOf(temp._2().get(1)/Stock.getTotalNoPeriod());
					Double profit             = closePriceAvg - openPriceAvg;
					
					return new Tuple2<>(symbol, profit);
			
				}
		
			};
			
			/* The Function2 stockProfit is used for a reduce() transformation.
			 * The reduce() takes as input the function name
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Profit.
			 * The function returns the Stock with the maximum profit.
			 * Logic:
			 * a. It compare 02 tuples and always returns the maximum of the tuple i.e. in this case the maximum profit.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Stock with maximum profit.
			 * */
			final Function2<Tuple2<String,Double>, Tuple2<String,Double>, Tuple2<String,Double>> stockProfit = new Function2<Tuple2<String,Double>, Tuple2<String,Double>, Tuple2<String,Double>>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Tuple2<String,Double> call(Tuple2<String,Double> arg0, Tuple2<String,Double> arg1) throws Exception {
					
					String symbol="";
					Double profit;
					
					if (arg0._2 > arg1._2()) {
						
						symbol = arg0._1();
						profit = arg0._2();				
						
					}else {
						
						symbol = arg1._1();
						profit = arg1._2();				
												
					}
							          
					return new Tuple2<>(symbol,profit);
	
				}
				
			};
			
			/* This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */				
			JavaDStream<Stock> stockData = jsonData.flatMap(jsonExtractor);		
			
			/* This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol, Open Price and Close Price from the Stock POJO.
			 * It creates a ArrayList<Double> and adds both the Open Price and Close Price for a Stock in the ArrayList.
			 * A Tuple2<String, List<Double>> is returned. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, List<Double>>.
			 */
			JavaPairDStream<String, List<Double>> stock = stockData.mapToPair(stockMapper);
			
			/* The reduceByKeyAndWindow() takes as input the following 03 arguments:
			 * a. Name of the function (of type Function2)
			 * b. Window Duration (in seconds)
			 * c. Sliding Window Duration (in seconds) 
			 * This transformation operates on JavaPairDStream<String, List<Double>> which contains Stock Symbol, Open Price and Close Price.
			 * The function aggregates the Open Prices and Close Prices of all stocks. 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * It creates a ArrayList<Double> and adds both the aggregated Open Prices and aggregated Close Prices for a Stock in the ArrayList.
			 * A Tuple2<String, List<Double>> is returned.
			 * The transformed RDDs are stored in a JavaPairDStream<String, List<Double>>
			 * */
			JavaPairDStream<String, List<Double>> stockSum = stock.reduceByKeyAndWindow(stockReducer,Durations.seconds(windowDuration*(long)secs), Durations.seconds(slidingWindowDuration*(long)secs));
			
			/* This transformation operates on JavaPairDStream<String, List<Double>> which contains the aggregated open price and close price of stocks.
			 * The PairFunction extracts the following data elements from Stock
			 * a. Symbol
			 * b. Open Price
			 * c, Close Price
			 * d. Total Number of Period (calculated in the JSON_EXTRACTOR function.
			 * The PairFunction calculates 
			 * a. The open price average by dividing the aggregated open price by total number of periods
			 * b. The close price average by dividing the aggregated close price by total number of periods.
			 * c. The profit by subtracting the open price average from the close price average.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Symbol and Profit which is calculated.
			 * */
			JavaPairDStream<String, Double> stockListProfit = stockSum.mapToPair(stockProfitList);
			
			/* The reduce() takes as input the function name
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Profit.
			 * The function returns the Stock with the maximum profit.
			 * Logic:
			 * a. It compare 02 tuples and always returns the maximum of the tuple i.e. in this case the maximum profit.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Stock with maximum profit.
			 * */
			JavaDStream<Tuple2<String, Double>> stockMaxProfit = stockListProfit.reduce(stockProfit);
			
			/* The final output i.e. Stock With Maximum Profit (Symbol, Profit) is printed on the console. 
			 * This will be printed once for each window 
			 * */
			stockMaxProfit.print();
			
			/* The final output i.e. Stock With Maximum Profit (Symbol, Profit) is saved to an output location on the local disk where the program is executed.
			 * The OUTPUT_STREAM_FOLDER_LOCATION points to the path where the output is to be generated.
			 * The output will be generated once for each window 
			 * */
			stockMaxProfit.dstream().repartition(1).saveAsTextFiles(outputStreamFolderLocation, "");
			
			jsc.start();
		
			jsc.awaitTermination();		
			
			jsc.close();
		
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally {
			
			try {
				
				if (jsc!=null) {
					
					jsc.close();
					
				}
				
			}catch(Exception e) {
				
				e.printStackTrace();
				
			}
		
		}		
		
	}
	
	public static void stocksWithRSI(String[] args) {
		
		/* This is the APP NAME to be set while initializing the SparkConf object */
		final String APP_NAME 				       = "SparkStreamingStockDataAnalysis3";
		
		/* This is the name of the analysis to be performed on streaming data*/
		final String ANALYSIS_NAME		           = "StocksWithRSI";
		
		/* This is the name of the checkpoint directory where data check-pointing will be carried out */
		final String CHECKPOINT_DIR                = "CHECKPOINT_DIR";

		/* The SECS variable contains value as 60. This value will be later used to convert seconds into minutes for Batch Interval, Window Duration and Sliding Window Duration.*/
		int secs								   = 60;
		
		/* The Batch Interval Is Defined To Be 01 Minute*/
		int batchInterval                          = 01;
		
		/* The Window Duration Is Defined To Be 10 Minutes*/
		int windowDuration                         = 10;
		
		/* The Sliding Window Duration Is Defined To Be 05 Minutes*/
		int slidingWindowDuration                  = 05;
		
		try {
			
			/* The first command-line value which points to the absolute path on the file system where streaming input data is stored. */
			inputStreamFolderLocation  = args[0];
			
			/* The second command-line value which points to absolute path on the file system where output data is used to be stored. */
			outputStreamFolderLocation = args[1]+ANALYSIS_NAME;
	
			/* The SparkConf object is created and initialized to run in local mode. An APPNAME is set for Spark to uniquely distinguish the spark streaming context for this program execution. */
			conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME);
		
			/* The JavaStreamingContext is created and initialized. The SparkConf object is passed along with Batch Interval (in seconds) */
			jsc = new JavaStreamingContext(conf, Durations.seconds(batchInterval*(long)secs));
			
			/* Introduced Logger to reduce Spark Logging on console */
			Logger.getRootLogger().setLevel(Level.ERROR);
		
			/* The textFileStream() method is used to fetch streaming data from the input location and create DStream of type JavaDStream<String>. The JSON data will be stored as String. */
			JavaDStream<String> jsonData = jsc.textFileStream(inputStreamFolderLocation).cache();
			
			/* The FlatMapFunction jsonExtractor  is used for a flatMap() transformation. 
			 * This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The class Stock is an implementation of a serializable POJO with composition of Price data.
			 * Logic:
			 * 1. The FlatMapFunction casts the JSON String into an object of type JSONArray
			 * 2. An ArrayList<Stock> is created and initialized
			 * 3. The JSONArray object is looped through to fetch the individual data elements and store them in an object of Stock type.
			 * 4. The Stock object is added to the Stock ArrayList.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			final FlatMapFunction<String, Stock> jsonExtractor = new FlatMapFunction<String, Stock>() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Stock> call(String istock)  {
					
					Stock stock;
					Price price;
					JSONObject priceData;
					    
					JSONArray jsonStock = (JSONArray) JSONSerializer.toJSON(istock);
				
					if (Stock.getTotalNoPeriod()>windowDuration) {
					
						Stock.setTotalNoPeriod(windowDuration);
					
					}else {
					
						Stock.incrTotalNoPeriod();
					
					}
				    
					ArrayList<Stock> stockArrList= new ArrayList<>(); 
					
					for(int i=0 ; i< jsonStock.size() ;i++) {
						
						stock = new Stock();
						price = new Price();
				    
						stock.setSymbol((String)jsonStock.getJSONObject(i).get("symbol"));
						stock.setTimestamp((String)jsonStock.getJSONObject(i).get("timestamp"));
					    priceData = jsonStock.getJSONObject(i).getJSONObject("priceData");
					    	
					    price.setOpen(Double.valueOf((String) priceData.get("open")));
					    price.setHigh(Double.valueOf((String) priceData.get("high")));
					    price.setLow(Double.valueOf((String) priceData.get("low")));
					    price.setClose(Double.valueOf((String) priceData.get("close")));
					    price.setVolume(Integer.valueOf((String) priceData.get("volume")));
					    	
					    stock.setPriceData(price);
					    	
					    stockArrList.add(stock);
		
					}
					
					return stockArrList.iterator();
				
				}
			};
		
			/* The PairFunction stockGainOrLoss is used for a mapToPair() transformation.
			 * This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol, Open Price and Close Price from the Stock POJO. 
			 * It calculates the gain_or_loss by subtracting open price from close price
			 * It returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			final PairFunction stockGainOrLoss = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Object arg0) throws Exception {
				
					Stock stock           = (Stock)arg0;
					
					String symbol         = stock.getSymbol();
					Double open           = stock.getPriceData().getOpen();
					Double close          = stock.getPriceData().getClose();
					Double gainOrLoss     = close - open;
					
					return new Tuple2<>(symbol, gainOrLoss);
			
				}
		
			};
		
			/* The Function stockGain is used for a filter() transformation.
			 * This transformation operates on JavaDStream<String, Double> which contains Stock Symbol and the difference stock_gain_or_loss calculated by subtracting open price from close price.
			 * The Function returns true if stock_gain_or_loss is greater than zero i.e. the stock_gain_or_loss is a profit.
			 * It stores an RDD in the resulting JavaPairDStream<String, Double> if the call method for that RDD returns true.
			 * */
			final Function<Tuple2<String, Double>, Boolean> stockGain = new Function<Tuple2<String, Double>, Boolean>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Boolean call(Tuple2<String, Double> arg0) throws Exception {
					
					return arg0._2() > 0.0;
	
				}
				
			};
			
			/* The Function stockLoss is used for a filter() transformation.
			 * This transformation operates on JavaDStream<String, Double> which contains Stock Symbol and the difference stock_gain_or_loss calculated by subtracting open price from close price.
			 * The Function returns true if stock_gain_or_loss is less than zero i.e. the stock_gain_or_loss is a loss.
			 * It stores an RDD in the resulting JavaPairDStream<String, Double> if the call method for that RDD returns true.
			 * */
			final Function<Tuple2<String, Double>, Boolean> stockLoss = new Function<Tuple2<String, Double>, Boolean>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Boolean call(Tuple2<String, Double> arg0) throws Exception {
					
					return arg0._2() < 0.0;
	
				}
				
			};	
			
			/* The Function2 stockAvgGain is used for a updateStateByKey() transformation.
			 * The updateStateByKey() takes as input the function name 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Gain.
			 * The function calculates the average stock gain for a particular window. 
			 * Logic:
			 * a. It retrieves the current gain
			 * b. It retrieves the previous gains (by pulling out the check-pointed data)
			 * c. It calculates the average gain using the following formula:
			 * average gain = ((previous gain*9)+current gain)/10)
			 * It returns an absolute value of the average gain for a particular Stock.
			 * This operation happens on a Key which is the Symbol 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			final Function2<List<Double>, Optional<Double>, Optional<Double>> stockAvgGain = new Function2<List<Double>, Optional<Double>, Optional<Double>>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
				public Optional<Double> call(List<Double> arg0, Optional<Double> arg1) throws Exception {
					
					double previousGain  = (double) arg1.or(0.0);
					double currentGain   = 0.0;
					double stockAvgGain;
					
					for (double i: arg0) {
						
						currentGain = currentGain + i;
					
					}
					
					stockAvgGain = ((previousGain*9)+currentGain)/10;
					
					return Optional.of(Math.abs(stockAvgGain));					
					
				}
				
			};
			
			/* The Function2 stockAvgLoss is used for a updateStateByKey() transformation.
			 * The updateStateByKey() takes as input the function name 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Loss
			 * The function calculates the average stock loss for a particular window. 
			 * Logic:
			 * a. It retrieves the current loss
			 * b. It retrieves the previous losses (by pulling out the check-pointed data)
			 * c. It calculates the average loss using the following formula:
			 * average loss = ((previous loss*9)+current loss)/10)
			 * It returns an absolute value of the average loss for a particular Stock.
			 * This operation happens on a Key which is the Symbol 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			final Function2<List<Double>, Optional<Double>, Optional<Double>> stockAvgLoss = new Function2<List<Double>, Optional<Double>, Optional<Double>>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
				public Optional<Double> call(List<Double> arg0, Optional<Double> arg1) throws Exception {
					
					double previousLoss  = (double) arg1.or(0.0);
					double currentLoss   = 0.0; 
					double stockAvgLoss;
					
					for (double i: arg0) {
						
						currentLoss = currentLoss + i;
					
					}
					
					stockAvgLoss = ((previousLoss*9)+currentLoss)/10;
					
					return Optional.of(Math.abs(stockAvgLoss));					
					
				}
				
			};
			
			/* The PairFunction rsi is used for a mapToPair() transformation.
			 * This transformation operates on JavaDStream<String, Double> which contains the stock data Symbol and average stock gain and average stock loss.
			 * The PairFunction extracts the data elements Symbol, average gain and average loss. 
			 * It calculates RSI using the following formula:
			 * RS = average gain / average loss
			 * RSI = 100 - (100/(1+RS))
			 * It returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			final PairFunction rsi = new PairFunction() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(Object arg0) throws Exception {
				
					Tuple2<String, Tuple2<Double, Double>> stock = (Tuple2<String, Tuple2<Double, Double>>) arg0;
					
					String symbol   = stock._1();
					double avgGain = stock._2()._1(); 
					double avgLoss = stock._2()._2();
					
					double rs = avgGain/avgLoss;
					
					double rsi = 100 - (100/(1+rs));
					
					return new Tuple2<>(symbol, rsi);
			
				}
		
			};
			
			/* A window is created on the incoming stream data with Window Duration as 10 minutes and Sliding Window Duration as 05 minutes. 
			 * All further transformations are performed within this defined window */
			JavaDStream<String> windowData = jsonData.window(Durations.seconds(windowDuration*(long)secs), Durations.seconds(slidingWindowDuration*(long)secs));
			
			/* This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			JavaDStream<Stock> stockData = windowData.flatMap(jsonExtractor);		
			
			/* This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol, Open Price and Close Price from the Stock POJO. 
			 * It calculates the gain_or_loss by subtracting open price from close price
			 * It returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			JavaPairDStream<String, Double> stockLossOrGain = stockData.mapToPair(stockGainOrLoss);
		
			/* For the purpose of data check-pointing so that data of previous RDDs is available, a check-point directory location is passed input to the checkpoint() method
			 * From this point onwards, data check-pointing is enabled. 
			 * */
			jsc.checkpoint(CHECKPOINT_DIR);
			
			/* This transformation operates on JavaDStream<String, Double> which contains Stock Symbol and the difference stock_gain_or_loss calculated by subtracting open price from close price.
			 * The Function returns true if stock_gain_or_loss is greater than zero i.e. the stock_gain_or_loss is a profit.
			 * It stores an RDD in the resulting JavaPairDStream<String, Double> if the call method for that RDD returns true.
			 * */
			JavaPairDStream<String, Double> stockGained = stockLossOrGain.filter(stockGain);
			
			/* The Function STOCK_LOSS is used for a filter() transformation.
			 * This transformation operates on JavaDStream<String, Double> which contains Stock Symbol and the difference stock_gain_or_loss calculated by subtracting open price from close price.
			 * The Function returns true if stock_gain_or_loss is less than zero i.e. the stock_gain_or_loss is a loss.
			 * It stores an RDD in the resulting JavaPairDStream<String, Double> if the call method for that RDD returns true.
			 * */
			JavaPairDStream<String, Double> stocksLoss = stockLossOrGain.filter(stockLoss);
			
			/* The updateStateByKey() takes as input the function name 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Gain.
			 * The function calculates the average stock gain for a particular window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			JavaPairDStream<String, Double> stockAvgGains = stockGained.updateStateByKey(stockAvgGain);
			
			/* The Function2 STOCK_AVG_LOSS is used for a updateStateByKey() transformation.
			 * The updateStateByKey() takes as input the function name 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Loss
			 * The function calculates the average stock loss for a particular window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			JavaPairDStream<String, Double> stockAvgLosses = stocksLoss.updateStateByKey(stockAvgLoss);
			
			/* The JavaPairDStreams containing average gain and average loss are joined together to get a JavaPairDStream containing both average gain and average loss. */
			JavaPairDStream<String, Tuple2<Double,Double>> avgGainLossCombined = stockAvgGains.join(stockAvgLosses);
			
			/* This transformation operates on JavaDStream<String, Double> which contains the stock data Symbol and average stock gain and average stock loss.
			 * The PairFunction extracts the data elements Symbol, average gain and average loss. 
			 * It calculates RSI using the following formula:
			 * RS = average gain / average loss
			 * RSI = 100 - (100/(1+RS))
			 * It returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			JavaPairDStream<String, Double> rsiComputed = avgGainLossCombined.mapToPair(rsi);
			
			/* The final output i.e. Stocks with RSI (Symbol, RSI) is printed on the console. 
			 * This will be printed once for each window 
			 * */
			rsiComputed.print();
			
			/* The final output i.e. Stocks with RSI (Symbol, RSI) are saved to an output location on the local disk where the program is executed.
			 * The OUTPUT_STREAM_FOLDER_LOCATION points to the path where the output is to be generated.
			 * The output will be generated once for each window 
			 * */
			rsiComputed.dstream().repartition(1).saveAsTextFiles(outputStreamFolderLocation, "");
			
			jsc.start();
			
			jsc.awaitTermination();		
			
			jsc.close();
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally {
			
			try {
				
				if (jsc!=null) {
					
					jsc.close();
					
				}
				
			}catch(Exception e) {
				
				e.printStackTrace();
				
			}
		
		}		
		
	}
	
	public static void stockWithMaximumVolume (String[] args) {
		
		/* This is the APP NAME to be set while initializing the SparkConf object */
		final String APP_NAME 				       = "SparkStreamingStockDataAnalysis4";
		
		/* This is the name of the analysis to be performed on streaming data*/
		final String ANALYSIS_NAME		           = "StockWithMaximumVolume";
		
		/* The Seconds variable contains value as 60. This value will be later used to convert seconds into minutes for Batch Interval, Window Duration and Sliding Window Duration.*/
		int secs								   = 60;
		
		/* The Batch Interval Is Defined To Be 01 Minute*/
		int batchInterval                          = 01;
		
		/* The Window Duration Is Defined To Be 10 Minutes*/
		int windowDuration                         = 10;
		
		/* The Sliding Window Duration Is Defined To Be 10 Minutes*/
		int slidingWindowDuration                  = 10;
		
		try {
			
			/* The first command-line value which points to the absolute path on the file system where streaming input data is stored. */
			inputStreamFolderLocation  = args[0];
			
			/* The second command-line value which points to absolute path on the file system where output data is used to be stored. */
			outputStreamFolderLocation = args[1]+ANALYSIS_NAME;
	
			/* The SparkConf object is re-initialized to run in local mode. An APPNAME is set for Spark to uniquely distinguish the spark streaming context for this program execution. */
			conf = new SparkConf().setMaster("local[*]").setAppName(APP_NAME);
		
			/* The JavaStreamingContext is re-initialized. The SparkConf object is passed along with Batch Interval (in seconds) */
			jsc = new JavaStreamingContext(conf, Durations.seconds(batchInterval*(long)secs));
			
			/* Introduced Logger to reduce Spark Logging on console */
			Logger.getRootLogger().setLevel(Level.ERROR);
		
			/* The textFileStream() method is used to fetch streaming data from the input location and create DStream of type JavaDStream<String>. The JSON data will be stored as String. */
			JavaDStream<String> jsonData = jsc.textFileStream(inputStreamFolderLocation).cache();
		
			/* The FlatMapFunction jsonExtractor  is used for a flatMap() transformation. 
			 * This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The class Stock is an implementation of a serializable POJO with composition of Price data.
			 * Logic:
			 * 1. The FlatMapFunction casts the JSON String into an object of type JSONArray
			 * 2. An ArrayList<Stock> is created and initialized
			 * 3. The JSONArray object is looped through to fetch the individual data elements and store them in an object of Stock type.
			 * 4. The Stock object is added to the Stock ArrayList.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			final FlatMapFunction<String, Stock> jsonExtractor = new FlatMapFunction<String, Stock>() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Stock> call(String istock)  {
					
					Stock stock;
					Price price;
					JSONObject priceData;
					    
					JSONArray jsonStock = (JSONArray) JSONSerializer.toJSON(istock);
				
					if (Stock.getTotalNoPeriod()>windowDuration) {
					
						Stock.setTotalNoPeriod(windowDuration);
					
					}else {
					
						Stock.incrTotalNoPeriod();
					
					}
				    
					ArrayList<Stock> stockArrList= new ArrayList<>(); 
					
					for(int i=0 ; i< jsonStock.size() ;i++) {
						
						stock = new Stock();
						price = new Price();
				    
					    stock.setSymbol((String)jsonStock.getJSONObject(i).get("symbol"));
					    stock.setTimestamp((String)jsonStock.getJSONObject(i).get("timestamp"));
					    priceData = jsonStock.getJSONObject(i).getJSONObject("priceData");
					    	
					    price.setOpen(Double.valueOf((String) priceData.get("open")));
					    price.setHigh(Double.valueOf((String) priceData.get("high")));
					    price.setLow(Double.valueOf((String) priceData.get("low")));
					    price.setClose(Double.valueOf((String) priceData.get("close")));
					    price.setVolume(Integer.valueOf((String) priceData.get("volume")));
					    	
					    stock.setPriceData(price);
					    	
					    stockArrList.add(stock);
		
					}
					
					return stockArrList.iterator();
				
				}
			};
		
			/* The PairFunction stockMapper is used for a mapToPair() transformation.
			 * This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol and Volume from the Stock POJO and returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			final PairFunction stockMapper = new PairFunction() {
			
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Integer> call(Object arg0) throws Exception {
				
					Stock st           = (Stock)arg0;
					String symbol      = st.getSymbol();
					Integer volume     = st.getPriceData().getVolume();
										
					return new Tuple2<>(symbol, volume);
			
				}
		
			};
		
			/* The Function2 stockReducer is used for a reduceByKeyAndWindow() transformation.
			 * The reduceByKeyAndWindow() takes as input the following 03 arguments:
			 * a. Name of the function (of type Function2)
			 * b. Window Duration (in seconds)
			 * c. Sliding Window Duration (in seconds) 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Volume.
			 * The function aggregates the Volume of all stocks based on Key (Symbol). 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			final Function2<Integer, Integer, Integer> stockReducer = new Function2<Integer, Integer, Integer>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Integer call(Integer arg0, Integer arg1) throws Exception {
		          
					return arg0+arg1;
	
				}
				
			};
					
			/* The Function2 stockVolume is used for a reduce() transformation.
			 * The reduce() takes as input the function name
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Volume
			 * The function returns the Stocks with the maximum volumes.
			 * Logic:
			 * a. It compare 02 tuples and always returns the maximum of the tuple i.e. in this case the maximum volumes.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Stocks with maximum volumes.
			 * */
			final Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>> stockVolume = new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
			  	
				private static final long serialVersionUID = 1L;

				@Override
		        public Tuple2<String,Integer> call(Tuple2<String,Integer> arg0, Tuple2<String,Integer> arg1) throws Exception {
					
					String symbol="";
					Integer volume=0;
					
					if (arg0._2 > arg1._2()) {
						
						symbol = arg0._1();
						volume = arg0._2();				
						
					}else {
						
						symbol = arg1._1();
						volume = arg1._2();				
												
					}
							          
					return new Tuple2<>(symbol,volume);
	
				}
				
			};
							
			/* This transformation operates on the JSON data collected as JavaDStream<String>.
			 * The JSON data is parsed to fetch individual data elements pertaining to each Stock (like, symbol, volume, etc) and are stored in an object of type Stock.
			 * The transformed RDDs are stored in a JavaDStream<Stock>.
			 * */
			JavaDStream<Stock> stockData = jsonData.flatMap(jsonExtractor);		
			
			/* This transformation operates on JavaDStream<Stock> which contains the stock data fetched and stored as Stock (POJO).
			 * The PairFunction extracts the data elements Symbol and Volume from the Stock POJO and returns a Key-Value pair of type Tuple2<String, Double>.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>.
			 * */
			JavaPairDStream<String, Integer> stock = stockData.mapToPair(stockMapper);
			
			/* The reduceByKeyAndWindow() takes as input the following 03 arguments:
			 * a. Name of the function (of type Function2)
			 * b. Window Duration (in seconds)
			 * c. Sliding Window Duration (in seconds) 
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Volume.
			 * The function aggregates the Volume of all stocks based on Key (Symbol). 
			 * The aggregation happens for all Stock records having the same Key in a particular window i.e. the aggregation happens on Key (Symbol) and within the Window. 
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double>
			 * */
			JavaPairDStream<String, Integer> stockVolumes = stock.reduceByKeyAndWindow(stockReducer, Durations.seconds(windowDuration*(long)secs), Durations.seconds(slidingWindowDuration*(long)secs));
			
			/* The reduce() takes as input the function name
			 * This transformation operates on JavaPairDStream<String, Double> which contains Stock Symbol and Volume
			 * The function returns the Stocks with the maximum volumes.
			 * Logic:
			 * a. It compare 02 tuples and always returns the maximum of the tuple i.e. in this case the maximum volumes.
			 * The transformed RDDs are stored in a JavaPairDStream<String, Double> which represents Stocks with maximum volumes.
			 * */
			JavaDStream<Tuple2<String, Integer>> stockVolumeSum = stockVolumes.reduce(stockVolume);
			
			/* The final output i.e. Stocks with maximum volumes (Symbol, Maximum Volume) is printed on the console. 
			 * This will be printed once for each window 
			 * */
			stockVolumeSum.print();
			
			/* The final output i.e. Stocks with maximum volumes (Symbol, Maximum Volume) is saved to an output location on the local disk where the program is executed.
			 * The OUTPUT_STREAM_FOLDER_LOCATION points to the path where the output is to be generated.
			 * The output will be generated once for each window 
			 * */
			stockVolumeSum.dstream().repartition(1).saveAsTextFiles(outputStreamFolderLocation, "");
			
			jsc.start();
			
			jsc.awaitTermination();		
			
			jsc.close();

			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally {
			
			try {
				
				if (jsc!=null) {
					
					jsc.close();
					
				}
				
			}catch(Exception e) {
				
				e.printStackTrace();
				
			}
		
		}

	}
	
}