import org.apache.spark.SparkConf;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.PreparedStatement;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MTAKafkaSparkStreaming {
    public static void main(String[] args) throws InterruptedException {
    	
    	String jdbcURL = "jdbc:hive2://localhost:10000/default"; // Change as per your Hive setup

        // JDBC connection properties
        String username = "cloudera"; // Replace with your Hive username
        String password = "cloudera"; // Replace with your Hive password

        // Hive table name
        String tableName = "mta_bus";
    	
        // Set Spark configuration
        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingExample").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // Set Kafka consumer properties
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker(s)
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-streaming-group"); // Consumer group ID
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // Read from the latest offset

        // Specify the Kafka topic to consume from
        Collection<String> topics = Arrays.asList("MTAKafkaTopic");

        // Create a DStream that represents the data from Kafka
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        // Process the Kafka stream
        kafkaStream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) -> {
            rdd.foreach(record -> {
                System.out.println("Received message: " + record.value());
                
                Object obj = new JSONParser().parse(record.value());
                JSONObject jo = (JSONObject) obj;
                
                
                JSONObject jo2 = ((JSONObject)jo.get("Siri"));
        		JSONObject jo3 = ((JSONObject)jo2.get("ServiceDelivery"));
        		
        		String ResponseTimestamp = (String) jo3.get("ResponseTimestamp");
        		
        		JSONArray ja = (JSONArray) jo3.get("VehicleMonitoringDelivery");
        		
        		
        		
        		
        		JSONObject jo4 = ((JSONObject)ja.get(0));
        		
        		
        		
        		
        		JSONArray ja2 = (JSONArray) jo4.get("VehicleActivity");
        		
        		try{
        			
        			
        			Class.forName("org.apache.hive.jdbc.HiveDriver");
                    // Create a JDBC connection to Hive
                    Connection connection = DriverManager.getConnection(jdbcURL, username, password);                    
                    // Create a Hive table
                    String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (VehicleRef STRING, PublishedLineName STRING , DestinationName STRING, ProgressRate STRING, ExpectedArrivalTime STRING, StopPointName STRING)";                    
                    Statement createTableStatement = connection.createStatement();
                    createTableStatement.execute(createTableSQL);                    
                    String insertDataSQL = "INSERT INTO " + tableName + " VALUES ";        	        	
        		for (int i=0;i<ja2.size();i++) {
        			JSONObject jo5 = ((JSONObject)ja2.get(i));
        			String RecordedAtTime = (String) jo5.get("RecordedAtTime");
        			JSONObject jo6 = ((JSONObject)jo5.get("MonitoredVehicleJourney"));
        			String PublishedLineName = (String) jo6.get("PublishedLineName");
        			String DestinationName = (String) jo6.get("DestinationName");        			        			
        			String ProgressRate = (String) jo6.get("ProgressRate");
        			String VehicleRef = (String) jo6.get("VehicleRef");        			        			
        			JSONObject jo7 = ((JSONObject)jo6.get("MonitoredCall"));        			
        			String ExpectedArrivalTime = (String) jo7.get("ExpectedArrivalTime");
        			String StopPointName = (String) jo7.get("StopPointName");        			        			        			        			
        			System.out.println("Vehicle "+VehicleRef+ " serving line " + PublishedLineName + " going to " + DestinationName + " has status "+ProgressRate + " is expected at "+ExpectedArrivalTime+" at stop "+StopPointName);        			
        			insertDataSQL=insertDataSQL+"('"+VehicleRef+"','"+PublishedLineName+"','"+DestinationName+"','"+ProgressRate+"','"+ExpectedArrivalTime+"','"+StopPointName+"'),";
        			// Insert JSON record into the table
                    //insertDataSQL = "\n"+insertDataSQL+ "INSERT INTO " + tableName + " VALUES('" + VehicleRef + "','"+PublishedLineName+"','"+ExpectedArrivalTime+"');";
        			
        			
                    
        			
        			
        			
        			}
        		
        		insertDataSQL=insertDataSQL.replaceAll(".$", "");
        		
        		
        		
                
                Statement insertDataStatement = connection.createStatement();
                insertDataStatement.execute(insertDataSQL);
                
               
                	

                    

                    // Close the connection
                    connection.close();
                } catch (ClassNotFoundException | SQLException e) {
                    e.printStackTrace();
                }
            });
        });

        // Start the Spark Streaming context
        streamingContext.start();

        // Wait for the application to terminate
        streamingContext.awaitTermination();
    }
}
