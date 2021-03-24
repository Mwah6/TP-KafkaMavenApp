import org.apache.kafka.clients.consumer.ConsumerConfig; 
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration; 
import java.util.Collections;
import java.util.Properties; 
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerApp {
//	private String KAFKA_BROKER_URL="192.168.43.22:9092"; private String TOPIC_NAME="testTopic";
	public static void main(String[] args) {
		new ConsumerApp();
	}
	public ConsumerApp() {
		Properties properties=new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG,"sample-group-test");// pas obligatoire
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");// permet de dire à Kafka de faire le commit dès qu'on fait un pull (sinon il attend l'odre "commit")
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");// 1000ms	
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"30000"); //expiration du suivi du topic si il n'y a plus de messages via le pull
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// est remplacé : properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// est remplacé : properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		//Par défaut, KafkaProducer utilise des clés au format String final KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
		 KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Collections.singletonList("test1")); // On peut faire un subscribe sur plusieurs topic => une liste
//		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{ en 1.8
		//scheduleAtFixedRate(command, initialDelay, period, unit)
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
			System.out.println("----------------------------------------");
			ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(1000));// Liste d'enregistremments
			consumerRecords.forEach(cr->{
			System.out.println("Key=>"+cr.key()+", Value=>"+cr.value()+", offset=>"+cr.offset());
			});
			},1000,1000, TimeUnit.MILLISECONDS);
		//, 1000 => Toutes les 1000ms, 1000 => commence dans 1000ms
	}
}


