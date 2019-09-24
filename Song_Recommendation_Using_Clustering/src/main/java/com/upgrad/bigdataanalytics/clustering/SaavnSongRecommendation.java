package com.upgrad.bigdataanalytics.clustering;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;

import scala.collection.Seq;

public class SaavnSongRecommendation {
	
	static ALS als                               = null;
	
	static ALSModel alsModel                     = null;

	static Dataset<Row> userClickStreamDS        = null;
	static Dataset<Row> userClickStreamCopyDS    = null;
	static Dataset<Row> newMetaDataDS            = null;
	static Dataset<Row> notificationClicksDS     = null;
	static Dataset<Row> notificationArtistsDS    = null;
	static Dataset<Row> featuresDS               = null;
	static Dataset<Row> scaledFeaturesDS         = null;
	static Dataset<Row> predictionsDS            = null;
	static Dataset<Row> userPredictionsDS        = null;
	static Dataset<Row> userClusterDS            = null;
	static Dataset<Row> userClickMetaDataDS      = null;
	static Dataset<Row> clusterArtistDS          = null;
	static Dataset<Row> userClusterArtistDS      = null;
	static Dataset<Row> userClusterNotficationDS = null;
	static Dataset<Row> denomDS 				 = null;
	static Dataset<Row> realNotfiyDS			 = null;
	static Dataset<Row> realNotificationCountDS  = null;
	static Dataset<Row> ctrDS 					 = null;

	
	static KMeans kMeans                         = null;
	
	static KMeansModel kMeansModel               = null;
	
	static List<Integer> numClusters             = null;

	static StandardScaler 	scaler               = null;
	
	static StandardScalerModel scalerModel       = null;

	static SparkSession sparkSession             = null;

	static StringIndexerModel userIDIndexer      = null;
	static StringIndexerModel songIDIndexer      = null;
	
	
	public static void initializeSpark(String appName) {
		
		try {			
			
			Logger.getLogger("org")
				  .setLevel(Level.ERROR);
			
			Logger.getLogger("akka")
				  .setLevel(Level.ERROR);

			sparkSession = SparkSession.builder()
									   .appName(appName)
									   .master("local[*]")
									   .getOrCreate();
		
		}catch(Exception e) {
		
			e.printStackTrace();
		
		}

	}
	
	
	public static void loadAndPrepareDataSet(String userClickStreamInputData, String newMetaInputData, String notificationClicksInputData, String notificationArtistsInputData){
		
		try {
		
			userClickStreamDS     = sparkSession.read()
												.format("csv")
												.option("header", false)
												.option("inferschema", true)
												.load(userClickStreamInputData);
			
			newMetaDataDS         = sparkSession.read()
												.format("csv")
												.option("header", false)
												.option("inferschema", true)
												.load(newMetaInputData);
			
			newMetaDataDS         = newMetaDataDS.select(newMetaDataDS.col("_c0").as("OriginalSongID"), 
														 newMetaDataDS.col("_c1").as("ArtistID"));
			
			notificationClicksDS  = sparkSession.read()
												.format("csv")
												.option("header", false)
												.option("inferschema", true)
												.load(notificationClicksInputData);
			
			notificationClicksDS  = notificationClicksDS.select(notificationClicksDS.col("_c0").as("ActualNotificationID"), 
																notificationClicksDS.col("_c1").as("OriginalUserID"));
			
			notificationArtistsDS = sparkSession.read()
												.format("csv")
												.option("header", false)
												.option("inferschema", true)
												.load(notificationArtistsInputData);
			
			notificationArtistsDS = notificationArtistsDS.select(notificationArtistsDS.col("_c0").as("NotificationID"), 
																 notificationArtistsDS.col("_c1").as("ArtistID"));
			userIDIndexer 		  = new StringIndexer().setInputCol("_c0")
					   								   .setOutputCol("UserID")
					   								   .fit(userClickStreamDS);

			songIDIndexer 		  = new StringIndexer().setInputCol("_c2")
													   .setOutputCol("SongID")
													   .fit(userClickStreamDS);	

			userClickStreamCopyDS = userIDIndexer.transform(userClickStreamDS);

			userClickStreamCopyDS = songIDIndexer.transform(userClickStreamCopyDS);

			userClickStreamCopyDS = userClickStreamCopyDS.select("UserID", "SongID", "_c0","_c2")
														 .groupBy("UserID", "SongID", "_c0","_c2")
														 .count();

			userClickStreamCopyDS = userClickStreamCopyDS.select(userClickStreamCopyDS.col("UserID"), 
								 								 userClickStreamCopyDS.col("SongID"), 
								 								 userClickStreamCopyDS.col("count").as("SongFrequency"),
								 								 userClickStreamCopyDS.col("_c0").as("OriginalUserID"), 
								 								 userClickStreamCopyDS.col("_c2").as("OriginalSongID"))
						 								 .repartition(20)
						 								 .cache();			
		
		}catch(Exception e) {
		
			e.printStackTrace();
		
		}
		
	}

	
	public static void performKMeansClustering() {
		
		try {
			
			als 		     = new ALS().setRank(10)
					 				 .setMaxIter(10)
					 				 .setRegParam(0.01)
					 				 .setImplicitPrefs(true)
					 				 .setUserCol("UserID")
					 				 .setItemCol("SongID")
					 				 .setRatingCol("SongFrequency");

			alsModel 	     = als.fit(userClickStreamCopyDS);

			userClickStreamCopyDS.unpersist();

			featuresDS       = alsModel.userFactors();

			UDF1 toVector    = new UDF1<Seq<Float>, Vector>(){

				private static final long serialVersionUID = 1L;

				public Vector call(Seq<Float> t1) throws Exception {

					List<Float> list = scala.collection.JavaConversions.seqAsJavaList(t1);

					double[] dblArry = new double[t1.length()];

					for (int i=0; i<list.size(); i++) {

						dblArry[i] = list.get(i);

					}

					return Vectors.dense(dblArry);
				}	

			};

			sparkSession.udf()
						.register("toVector", toVector, new VectorUDT());

			featuresDS 	     = featuresDS.withColumn("NewFeatures", 
								  				 functions.callUDF("toVector",featuresDS.col("features")));

			featuresDS       = featuresDS.select(featuresDS.col("id"), 
							  				  featuresDS.col("NewFeatures"));

			scaler           = new StandardScaler().setInputCol("NewFeatures")
												.setOutputCol("ScaledFeatures")
												.setWithStd(true)
												.setWithMean(true);

			scalerModel      = scaler.fit(featuresDS);

			scaledFeaturesDS = scalerModel.transform(featuresDS);

			scaledFeaturesDS = scaledFeaturesDS.select(scaledFeaturesDS.col("id"),
													   scaledFeaturesDS.col("ScaledFeatures").as("features"))
											   .repartition(16)
											   .cache();

			/*numClusters      = Arrays.asList(20,40,60,80,100,120,140,160,180,200);

			for (Integer K : numClusters) {

				kMeans = new KMeans().setK(K).setSeed(1);

				kMeansModel = kMeans.fit(scaledFeaturesDS);

				double wsse = kMeansModel.computeCost(scaledFeaturesDS);

				System.out.println("WSSE For K: "+K+" :"+wsse);

				Vector[] centers = kMeansModel.clusterCenters();

				System.out.println("Cluster Centers: ");

				for (Vector center: centers) {

					System.out.println(center);

				}

			}*/

			kMeans           = new KMeans().setK(40).setSeed(1);

			kMeansModel      = kMeans.fit(scaledFeaturesDS);

			predictionsDS    = kMeansModel.transform(scaledFeaturesDS);

			predictionsDS    = predictionsDS.select(predictionsDS.col("id").as("UserID").cast("Double"), 
													predictionsDS.col("Prediction"));

			scaledFeaturesDS.unpersist();
		
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
	}

	
	public static void calculateUserClusters() {
		
		try {
			
			predictionsDS.createOrReplaceTempView("Predictions");
			
			userClickStreamCopyDS.createOrReplaceTempView("UserClickStream");
			
			userPredictionsDS = sparkSession.sql("select UserClickStream.UserID, "
													  + "UserClickStream.SongID, "
													  + "Predictions.Prediction, "
													  + "UserClickStream.OriginalUserID, "
													  + "UserClickStream.OriginalSongID "
													  + "from UserClickStream "
													  + "join Predictions "
													  + "on UserClickStream.UserID = Predictions.UserID");

			userPredictionsDS = userPredictionsDS.select("OriginalUserID", "OriginalSongID", "Prediction").distinct();
			
			userPredictionsDS.createOrReplaceTempView("UserPredictions");
			
			userPredictionsDS.cache();
			
			userClusterDS     = userPredictionsDS.select(userPredictionsDS.col("OriginalUserID"), 
														 userPredictionsDS.col("Prediction"));
			
		}catch(Exception e) {
			
			e.printStackTrace();
		}
		
	}
	
	
	public static void calculatePopularArtist() {
		
		try {
		
			newMetaDataDS.createOrReplaceTempView("NewMetaData");
			
			
			userClickMetaDataDS      = sparkSession.sql("select a.*, b.ArtistID "
													  + "from UserPredictions a "
													  + "join NewMetaData b on "
													  + "a.OriginalSongID = b.OriginalSongID");
			
			userPredictionsDS.unpersist();
			
			userClickMetaDataDS      = userClickMetaDataDS.groupBy("Prediction", "ArtistID").count();
			userClickMetaDataDS      = userClickMetaDataDS.select(userClickMetaDataDS.col("Prediction"), 
																  userClickMetaDataDS.col("ArtistID"), 
																  userClickMetaDataDS.col("count").as("Frequency"));
	
			userClickMetaDataDS.createOrReplaceTempView("UserClickMetaData");
			
			clusterArtistDS          = sparkSession.sql("select UserClickMetaData.* "
													  + "from UserClickMetaData join "
													  + "(select Prediction, "
													  + "MAX(UserClickMetaData.Frequency) as CountMax "
													  + "from UserClickMetaData "
													  + "group by Prediction) as UserClickMetaData1 "
													  + "on UserClickMetaData.Prediction = UserClickMetaData1.Prediction "
													  + "and UserClickMetaData.Frequency = UserClickMetaData1.CountMax");
			
			clusterArtistDS.orderBy(clusterArtistDS.col("Frequency").desc());
			
			userClickMetaDataDS.unpersist();
			
			userClusterArtistDS      = userClusterDS.join(clusterArtistDS, 
														  userClusterDS.col("Prediction").equalTo(clusterArtistDS.col("Prediction")),
														  "inner").drop(userClusterDS.col("Prediction"))
													.drop(clusterArtistDS.col("Frequency"));

		}catch(Exception e) {
			
			e.printStackTrace();

		}
		
	}
	
	
	public static void writeNotificationIDForPopularArtist(String userClustersOutputData) {
	
		try {
			
			userClusterNotficationDS = userClusterArtistDS.join(notificationArtistsDS,
					userClusterArtistDS.col("ArtistID").equalTo(notificationArtistsDS.col("ArtistID")),
					"inner").drop(userClusterArtistDS.col("ArtistID"));

			userClusterNotficationDS.cache();
			
			userClusterNotficationDS = userClusterNotficationDS.drop(userClusterNotficationDS.col("Prediction"));
			
			userClusterNotficationDS.repartition(userClusterNotficationDS.col("NotificationID"))
									.write()
									.partitionBy("NotificationID")
									.mode(SaveMode.Overwrite)
									.csv(userClustersOutputData);
			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
	
	}
	
	
	public static void writeCTRForEachNotification(String ctrOutputData) {
	
		try {

			denomDS 			     = userClusterNotficationDS.groupBy("NotificationID").count();
			
			denomDS                  = denomDS.select(denomDS.col("NotificationID"), denomDS.col("count").as("Denominator"));
			
			denomDS                  = denomDS.select(denomDS.col("NotificationID"), denomDS.col("Denominator"));

			userClusterNotficationDS.createOrReplaceTempView("UserClusterNotfication");
			
			notificationClicksDS.createOrReplaceTempView("NotificationClicks");
			
			realNotfiyDS             = sparkSession.sql("select a.*, "
													  + "b.ActualNotificationID "
													  + "from UserClusterNotfication a inner join "
													  + "NotificationClicks b on (a.OriginalUserID = b.OriginalUserID) "
													  + "and b.ActualNotificationID = a.NotificationID");
			
			userClusterNotficationDS.unpersist();
			
			realNotfiyDS.createOrReplaceTempView("RealNotfiy");
			
			realNotificationCountDS  = sparkSession.sql("select ActualNotificationID, "
													  + "count(*) as Numerator "
													  + "from RealNotfiy "
													  + "group by ActualNotificationID");
			
			realNotificationCountDS.createOrReplaceTempView("RealNotificationCount");
			
			denomDS.createOrReplaceTempView("Denom");
			
			ctrDS 					 = sparkSession.sql("select a.ActualNotificationID, "
													  + "(a.Numerator / b.Denominator)*100 as CTR "
													  + "from RealNotificationCount a inner join Denom b "
													  + "on a.ActualNotificationID = b.NotificationID");
			
			ctrDS.write()
				 .mode(SaveMode.Overwrite)
				 .csv(ctrOutputData);

			
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}
		
	}
	
	
	public static void main(String[] args) {
	
		String appName                        = "SaavnSongRecommendation";
		
		String userClickStreamInputData       = args[0].trim();
		String newMetaInputData               = args[1].trim();
		String notificationClicksInputData    = args[2].trim();
		String notificationArtistsInputData   = args[3].trim();
		String ctrOutputData                  = args[4].trim();
		String userClustersOutputData         = args[5].trim();
				
		try {
			
			if (userClickStreamInputData!=null && newMetaInputData!=null && notificationClicksInputData!=null && notificationArtistsInputData!=null && ctrOutputData!=null && userClustersOutputData!=null) {
			
				initializeSpark(appName);
			
				loadAndPrepareDataSet(userClickStreamInputData, newMetaInputData, notificationClicksInputData, notificationArtistsInputData);
			
				performKMeansClustering();
			
				calculateUserClusters();					

				calculatePopularArtist();
			
				writeNotificationIDForPopularArtist(userClustersOutputData);
			
				writeCTRForEachNotification(ctrOutputData);
				
			}else {
				
				System.out.println("The program has ended as all or one of the command-line parameters have not been specified.");
				
			}
			
		}
		catch (Exception e) {
			
			e.printStackTrace();
			
		}finally {
		
			try {
				
				if (sparkSession!=null) {
					
					sparkSession.close();
				
				}
				
			}catch(Exception e) {
				
				e.printStackTrace();
			
			}
		
		}
     	
	}

}