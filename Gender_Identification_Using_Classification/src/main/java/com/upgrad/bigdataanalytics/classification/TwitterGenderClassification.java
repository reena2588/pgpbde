package com.upgrad.bigdataanalytics.classification;

import static org.apache.spark.sql.functions.col;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TwitterGenderClassification {

	static Dataset<Row> originalDS           = null;
	
	static Dataset<Row> selectedFeaturesDS   = null;
	static Dataset<Row> finalFeaturesDS      = null;
	
	static Dataset<Row> trainingDS           = null;
    static Dataset<Row> testingDS            = null;
    
    static Dataset<Row> trainingNBDS         = null;
	static Dataset<Row> testingNBDS          = null;
	
	static Dataset<Row> trainingDTCDS        = null;
	static Dataset<Row> testingDTCDS         = null;
	
	static Dataset<Row> trainingRFCDS        = null;
	static Dataset<Row> testingRFCDS         = null;
	
    static SparkSession sparkSession         = null;
    
    static String[] labels                   = null;
	
	public static void initializeSpark() {
		
		try {			
		
			Logger.getLogger("org").setLevel(Level.ERROR);
			Logger.getLogger("akka").setLevel(Level.ERROR);

			sparkSession = SparkSession.builder().appName("TwitterGenderClassification").master("local[*]").getOrCreate();
		
		}catch(Exception e) {
		
			e.printStackTrace();
		
		}

	}
	
	public static void loadAndPrepareDataSet(String inputPath){
	
		String[] unwantedColumns = new String[20];
		
		try {
		
			originalDS = sparkSession.read()
									 .option("header", true)
									 .option("multiLine", true)
									 .option("mode", "DROPMALFORMED")
									 .option("inferschema", true)
									 .csv(inputPath);
		
			unwantedColumns[0] = "_unit_id";
			unwantedColumns[1] = "_golden";
			unwantedColumns[2] = "_last_judgment_at";
			unwantedColumns[3] = "profile_yn";
			unwantedColumns[4] = "profile_yn:confidence";
			unwantedColumns[5] = "created";
			unwantedColumns[6] = "fav_number";
			unwantedColumns[7] = "gender_gold";
			unwantedColumns[8] = "link_color";
			unwantedColumns[9] = "name";
			unwantedColumns[10] = "profile_yn_gold";
			unwantedColumns[11] = "profileimage";
			unwantedColumns[12] = "retweet_count";
			unwantedColumns[13] = "sidebar_color";
			unwantedColumns[14] = "tweet_coord";
			unwantedColumns[15] = "tweet_count";
			unwantedColumns[16] = "tweet_created";
			unwantedColumns[17] = "tweet_id";
			unwantedColumns[18] = "tweet_location";
			unwantedColumns[19] = "user_timezone";
			

			for(String col: unwantedColumns) {

				originalDS = originalDS.drop(col);

			}

			originalDS = originalDS.na().drop();

			originalDS = originalDS.filter(col("gender").notEqual("unknown")).filter(col("gender").notEqual(""));
		
		}catch(Exception e) {
		
			e.printStackTrace();
		
		}
		
	}
	
	public static void featureEngineeringAndEncoding(){

		Dataset<Row> featuresDS                     = null;
		
		HashingTF descriptionHTF                    = null;
		
        HashingTF textHTF                    		= null;
        
        IDF descriptionIDF                          = null;
        
        IDF textIDF                          		= null;
        
        OneHotEncoderEstimator ohee                 = null;
        
        OneHotEncoderModel oheeModel                = null;
        
        Pipeline featurePipeline	                = null;

        PipelineModel featurePipelineModel    		= null;
        
		StringIndexerModel labelIndexer      		= null;
		
		StringIndexerModel trustedJudgementsIndexer = null;
		
		StringIndexerModel unitStateIndexer         = null;
		
		StringIndexerModel genderConfidenceIndexer  = null;
		
		StopWordsRemover removerDescription         = null;
		
		StopWordsRemover removerText         		= null;

		Tokenizer descriptionTokenizer              = null;
		
		Tokenizer textTokenizer              		= null;
		
		
		try {
			
			labelIndexer  				= new StringIndexer()
			    	   						.setInputCol("gender")
			    	   						.setOutputCol("label")
			    	   						.fit(originalDS);				

			trustedJudgementsIndexer  	= new StringIndexer()
 	   										.setInputCol("_trusted_judgments")
 	   										.setOutputCol("_trusted_judgments_index")
 	   										.fit(originalDS);

			unitStateIndexer  			= new StringIndexer()
											.setInputCol("_unit_state")
											.setOutputCol("_unit_state_index")
											.fit(originalDS);

			genderConfidenceIndexer  	= new StringIndexer()
											.setInputCol("gender:confidence")
											.setOutputCol("gender:confidence_index")
											.fit(originalDS);

			textTokenizer 		 		= new Tokenizer()
				       						.setInputCol("text")
				       						.setOutputCol("textWords");

			removerText 		 		= new StopWordsRemover()
 				   							.setInputCol(textTokenizer.getOutputCol())
 				   							.setOutputCol("filteredText");		

			textHTF 	 		 		= new HashingTF()
					   						.setNumFeatures(1000)
					   						.setInputCol(removerText.getOutputCol())
					   						.setOutputCol("numTextFeatures");

			textIDF 			 		= new IDF()
					   						.setInputCol(textHTF.getOutputCol())
					   						.setOutputCol("textFeatures");

			descriptionTokenizer 		= new Tokenizer()
											.setInputCol("description")
											.setOutputCol("descriptionWords");

			removerDescription 			= new StopWordsRemover()
											.setInputCol(descriptionTokenizer.getOutputCol())
											.setOutputCol("filteredDescription");		

			descriptionHTF 				= new HashingTF()
											.setNumFeatures(1000)
											.setInputCol(removerDescription.getOutputCol())
											.setOutputCol("numDescriptionFeatures");

			descriptionIDF 				= new IDF()
											.setInputCol(descriptionHTF.getOutputCol())
											.setOutputCol("descriptionFeatures");

			featurePipeline	  		   	= new Pipeline()
	     				 					.setStages(new PipelineStage[] {labelIndexer, 
	     				 													trustedJudgementsIndexer, 
	     				 													unitStateIndexer, 
	     				 													genderConfidenceIndexer, 
	     				 													textTokenizer, 
	     				 													removerText, 
	     				 													textHTF, 
	     				 													textIDF, 
	     				 													descriptionTokenizer, 
	     				 													removerDescription, 
	     				 													descriptionHTF, 
	     				 													descriptionIDF});	

			featurePipelineModel = featurePipeline.fit(originalDS);

			featuresDS 			 = featurePipelineModel.transform(originalDS);


			ohee 	  = new OneHotEncoderEstimator().setInputCols(new String [] {"_trusted_judgments_index", 
																				 "_unit_state_index", 
																				 "gender:confidence_index"})
													.setOutputCols(new String[] {"categoryVec1", 
																			  	 "categoryVec2", 
																			  	 "categoryVec3"});

			oheeModel = ohee.fit(featuresDS);

	
			labels = new String[labelIndexer.labels().length];
			
			labels = Arrays.copyOf(labelIndexer.labels(),labelIndexer.labels().length);
			
			selectedFeaturesDS = oheeModel.transform(featuresDS);			
	       
			
		}catch(Exception e) {
			
			e.printStackTrace();
	
		}
	
	}
	
	public static void featureAssemblingAndSelection(){
		
		ChiSqSelector selector    = null;
		VectorAssembler assembler = null;
		
		try {
			
			assembler = new VectorAssembler()
	                		.setInputCols(new String[]{"categoryVec1", "categoryVec2", "categoryVec3", "textFeatures", "descriptionFeatures"})
	                		.setOutputCol("features");
	        
	        finalFeaturesDS = assembler.transform(selectedFeaturesDS).select("label","features","gender");
	        
	        
			selector = new ChiSqSelector();
			
			selector.setNumTopFeatures(1000);
			selector.setLabelCol("label");
			selector.setFeaturesCol("features");
			selector.setOutputCol("finalFeatures");
			
			finalFeaturesDS = selector.fit(finalFeaturesDS).transform(finalFeaturesDS);
			
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void splitDS() {
		
		Dataset<Row>[] splits = null;
		
		try {
			
	        splits      = finalFeaturesDS.randomSplit(new double[] { 0.8, 0.2 }, 1L);
	        
			trainingDS = splits[0];
			
			testingDS  = splits[1];
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void modelOnNaiveBayesAlgo() {
		
		IndexToString labelConverter = null;
		NaiveBayes nb                = null;
		NaiveBayesModel nbModel      = null;
		
		try {
			
			nb = new NaiveBayes().setLabelCol("label").setFeaturesCol("finalFeatures");
			
			nbModel = nb.fit(trainingDS);
	        
			trainingNBDS = nbModel.transform(trainingDS);
			
			testingNBDS = nbModel.transform(testingDS);
			
			labelConverter = new IndexToString()
					   			.setInputCol("prediction")
					   			.setOutputCol("predictedLabel")
					   			.setLabels(labels);		   
	        
			testingNBDS = labelConverter.transform(testingNBDS);
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
	}
	
	public static void modelOnDecisionTreeAlgo() {
		
		DecisionTreeClassifier dtc               = null;
		DecisionTreeClassificationModel dtcModel = null;
		
		IndexToString labelConverter             = null;
		
		try {
			
			dtc 	 = new DecisionTreeClassifier()
	        			.setLabelCol("label")
	        			.setFeaturesCol("finalFeatures")
	        			.setMaxDepth(30);
			
			dtcModel = dtc.fit(trainingDS);		
	    	
			trainingDTCDS = dtcModel.transform(trainingDS);
			
			testingDTCDS = dtcModel.transform(testingDS);
			
			labelConverter = new IndexToString()
		   						.setInputCol("prediction")
		   						.setOutputCol("predictedLabel")
		   						.setLabels(labels);		   

			testingDTCDS = labelConverter.transform(testingDTCDS);
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
	}
	
	public static void modelOnRandomForrestAlgo() {

		IndexToString labelConverter                = null;
		
        RandomForestClassifier rfc           		= null;
        RandomForestClassificationModel rfcModel    = null;
        		
		try {
			
		    rfc = new RandomForestClassifier()
	        			.setImpurity("gini")
	        			.setMaxDepth(30)
	        			.setFeatureSubsetStrategy("auto")
	        			.setLabelCol("label")
	        			.setFeaturesCol("finalFeatures");
			
		    rfcModel = rfc.fit(trainingDS);		
	    	
		    trainingRFCDS = rfcModel.transform(trainingDS);
			
		    testingRFCDS = rfcModel.transform(testingDS);
		    
			labelConverter = new IndexToString()
						.setInputCol("prediction")
						.setOutputCol("predictedLabel")
						.setLabels(labels);		   

			testingRFCDS = labelConverter.transform(testingRFCDS);

			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void evaluateModelOnNaiveBayesAlgo() {
		
		double accuracyNBTrain; 
		double accuracyNBTest;
		double weightedPrecisionNBTest;
		double weightedRecallNBTest;
		double f1ScorelNBTest;
		
		MulticlassClassificationEvaluator evaluator = null;
		
		try {
			
			evaluator = new MulticlassClassificationEvaluator()
					 		.setLabelCol("label")
					 		.setPredictionCol("prediction");	
			
			accuracyNBTrain         = evaluator.setMetricName("accuracy").evaluate(trainingNBDS);
			accuracyNBTest          = evaluator.setMetricName("accuracy").evaluate(testingNBDS);
			weightedPrecisionNBTest = evaluator.setMetricName("weightedPrecision").evaluate(testingNBDS);
			weightedRecallNBTest    = evaluator.setMetricName("weightedRecall").evaluate(testingNBDS);
			f1ScorelNBTest          = evaluator.setMetricName("f1").evaluate(testingNBDS);
			
			System.out.println("Naive Bayes Classifier:");
			System.out.println("-----------------------");
			System.out.println("");
			
			/*System.out.println("Training Data Accuracy = " + Math.round(accuracyNBTrain * 100) + " %");
			System.out.println("");*/
			
			System.out.println("Accuracy = " + Math.round(accuracyNBTest * 100) + " %");
			System.out.println("");
			
    		System.out.println("Weighted Precision  = " + Math.round(weightedPrecisionNBTest * 100) + " %");
			System.out.println("");
			
    		System.out.println("Weighted Recall = " + Math.round(weightedRecallNBTest * 100) + " %");
			System.out.println("");
       		
    		System.out.println("F1 Score = " + Math.round(f1ScorelNBTest * 100) + " %");
			System.out.println("");
			
       		System.out.println("Confusion Matrix:");
       		testingNBDS.groupBy(col("gender"), col("predictedLabel")).count().show();
 
			System.out.println("");
			System.out.println("");
			System.out.println("");
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void evaluateModelOnDecisionTreeAlgo() {

		double accuracyDTCTrain; 
		double accuracyDTCTest;
		double weightedPrecisionDTCTest;
		double weightedRecallDTCTest;
		double f1ScorelDTCTest;
		
		MulticlassClassificationEvaluator evaluator = null;

		try {
			
			evaluator = new MulticlassClassificationEvaluator()
			 				.setLabelCol("label")
			 				.setPredictionCol("prediction");

			accuracyDTCTrain         = evaluator.setMetricName("accuracy").evaluate(trainingDTCDS);
			accuracyDTCTest          = evaluator.setMetricName("accuracy").evaluate(testingDTCDS);
			weightedPrecisionDTCTest = evaluator.setMetricName("weightedPrecision").evaluate(testingDTCDS);
			weightedRecallDTCTest    = evaluator.setMetricName("weightedRecall").evaluate(testingDTCDS);
			f1ScorelDTCTest          = evaluator.setMetricName("f1").evaluate(testingDTCDS);
			
			
			System.out.println("Decision Tree Classifier:");
			System.out.println("-------------------------");
			System.out.println("");

			/*System.out.println("Training Data Accuracy = " + Math.round(accuracyDTCTrain * 100) + " %");
			System.out.println("");*/
			
			System.out.println("Accuracy = " + Math.round(accuracyDTCTest * 100) + " %");
			System.out.println("");
			
    		System.out.println("Weighted Precision  = " + Math.round(weightedPrecisionDTCTest * 100) + " %");
			System.out.println("");
      		
    		System.out.println("Weighted Recall = " + Math.round(weightedRecallDTCTest * 100) + " %");
			System.out.println("");
       		
    		System.out.println("F1 Score = " + Math.round(f1ScorelDTCTest * 100) + " %");
			System.out.println("");
			
       		System.out.println("Confusion Matrix:");
       		testingDTCDS.groupBy(col("gender"), col("predictedLabel")).count().show();
 
			System.out.println("");
			System.out.println("");
			System.out.println("");

		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void evaluateModelOnRandomForrestAlgo() {
		
		double accuracyRFCTrain; 
		double accuracyRFCTest;
		double weightedPrecisionRFCTest;
		double weightedRecallRFCTest;
		double f1ScorelRFCTest;
		
		
		MulticlassClassificationEvaluator evaluator = null;
		
		try {
			
			evaluator = new MulticlassClassificationEvaluator()
			 				.setLabelCol("label")
			 				.setPredictionCol("prediction");	
			
			accuracyRFCTrain         = evaluator.setMetricName("accuracy").evaluate(trainingRFCDS);
			accuracyRFCTest          = evaluator.setMetricName("accuracy").evaluate(testingRFCDS);
			weightedPrecisionRFCTest = evaluator.setMetricName("weightedPrecision").evaluate(testingRFCDS);
			weightedRecallRFCTest    = evaluator.setMetricName("weightedRecall").evaluate(testingRFCDS);
       		f1ScorelRFCTest          = evaluator.setMetricName("f1").evaluate(testingRFCDS);

			
			System.out.println("Random Forrest Classifier:");
			System.out.println("--------------------------");
			System.out.println("");
			
			/*System.out.println("Training Data Accuracy = " + Math.round(accuracyRFCTrain * 100) + " %");
			System.out.println("");*/
			
			System.out.println("Accuracy = " + Math.round(accuracyRFCTest * 100) + " %");
			System.out.println("");
			
			System.out.println("Weighted Precision  = " + Math.round(weightedPrecisionRFCTest * 100) + " %");
			System.out.println("");
			
      		System.out.println("Weighted Recall = " + Math.round(weightedRecallRFCTest * 100) + " %");
			System.out.println("");
			
    		System.out.println("F1 Score = " + Math.round(f1ScorelRFCTest * 100) + " %");
			System.out.println("");
			
       		System.out.println("Confusion Matrix:");
       		testingRFCDS.groupBy(col("gender"), col("predictedLabel")).count().show();
 
			System.out.println("");
			System.out.println("");
			System.out.println("");
			
		}catch(Exception e) {
			
			e.printStackTrace();
			
		}
		
	}
	
	public static void main(String[] args) {		
     		
		String inputPath = args[0];
	
		try {
			
			initializeSpark();
			
			loadAndPrepareDataSet(inputPath);
			
			featureEngineeringAndEncoding();
			
			featureAssemblingAndSelection();
		
			splitDS();
	        
			modelOnNaiveBayesAlgo();
			
			modelOnDecisionTreeAlgo();
			
			modelOnRandomForrestAlgo();
			
			evaluateModelOnNaiveBayesAlgo();
			
			evaluateModelOnDecisionTreeAlgo();
			
			evaluateModelOnRandomForrestAlgo();
	   		
		}catch(Exception e) {
			
			e.printStackTrace();
		
		}finally{
		
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