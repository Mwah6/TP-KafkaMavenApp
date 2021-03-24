import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties; 
import java.util.concurrent.Executors; 
import java.util.concurrent.TimeUnit;

public class ProducerApp {
	private int counter;
	//private String KAFKA_BROKER_URL="192.168.43.22:9092";
	private String TOPIC_NAME="test1"; 
	//private String clientID="client_prod_1";
	
	public static void main(String[] args) {
		new ProducerApp();
	}
	public ProducerApp() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "client-producer-1");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//properties.put("bootstrap.servers", KAFKA_BROKER_URL);
		//properties.put("client.id", clientID);
		//properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		//properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
			//++counter;
			String msg=String.valueOf(Math.random()*1000);
			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, "producer "+ ++counter,msg),
					(metadata,ex)->{ //metadata et gestion de l'exception
						//metadata metadonnées envoyées par Kafka vers (information placée dans telle partition...
						System.out.println("Sending Message key=>"+counter+" Value =>"+msg);
						System.out.println("Partition => "+metadata.partition()+" Offset=>"+metadata.offset());
					});
		},500,1, TimeUnit.MILLISECONDS);
	}
}