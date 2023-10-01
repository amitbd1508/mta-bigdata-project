import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ProducerMTAKafka {
    private static final Logger log = LoggerFactory.getLogger(ProducerMTAKafka.class);
    
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        
        
        String restServiceUrl = "https://api.prod.obanyc.com/api/siri/vehicle-monitoring.json?key=e4ef4883-a15f-4556-b421-760851365189&LineRef=MTABC_Q23";
        
     while(true){
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Create an HTTP client
        	
        	
        	
            HttpClient httpClient = new DefaultHttpClient();
            HttpGet httpGet = new HttpGet(restServiceUrl);

            // Execute the HTTP request
            HttpResponse response = httpClient.execute(httpGet);

            // Check if the response status code is OK (200)
            if (response.getStatusLine().getStatusCode() == 200) {
                // Extract the JSON response content
                String jsonResponse = EntityUtils.toString(response.getEntity());

                // Send the JSON response to Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>("MTAKafkaTopic", jsonResponse);
                producer.send(record);

                System.out.println("JSON data sent to Kafka topic successfully.");
                
                producer.flush();
                
                
            } else {
                System.err.println("HTTP Request failed with status code: " + response.getStatusLine().getStatusCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close(); // Close the Kafka producer when done
        }
        
        try {
            //sending the actual Thread of execution to sleep X milliseconds
            Thread.sleep(120000);
        } catch(InterruptedException ie) {}
        
     }
        
        
        
        }
        
        /*// create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("FirstKafkaTopic", "hello world from java2");

        // send data - asynchronous
        producer.send(producerRecord);

        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();*/
    
}
