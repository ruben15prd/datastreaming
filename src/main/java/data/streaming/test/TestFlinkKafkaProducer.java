
package data.streaming.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.aaux.ValidTagsTweetEndpoIntinitializer;
import data.streaming.utils.LoggingFactory;
import data.streaming.utils.Utils;

public class TestFlinkKafkaProducer {

	private static final Integer PARALLELISM = 2;

	public static void main(String... args) throws Exception {

		TwitterSource twitterSource = new TwitterSource(LoggingFactory.getTwitterCredentias());
		
		
		
		
		
		// Creating a Mongo client
				MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

				MongoClient client = new MongoClient(uri);

				MongoDatabase db = client.getDatabase(uri.getDatabase());
				
				

				// Retrieving a collection

				MongoCollection<Document> patents = db.getCollection("patents");
				
				
				
				BasicDBObject regexQuery = new BasicDBObject();
				
	        	regexQuery.put("keywords.0",
	        		new BasicDBObject("$exists", "true"));
				
				List<Document> documents = (List<Document>) patents.find(regexQuery).into(
						new ArrayList<Document>());
				
				client.close();
				Set<String> tags = new HashSet<String>();
				// Recorremos todos los keywords de las patentes
		               for(Document document : documents){
		            	   List<String> keywords = (List<String>) document.get("keywords");
		                   if(keywords.size() > 0) {
		                	   
		                	   for(String s:keywords) {
			                		   tags.add(s);
		                	   }
		                   }
		            	   
		               }
		   
		        //Generamos un array con los keywords
		        System.out.println("Numero de tags a buscar: "+ tags.size());      
		        //String[] tagNames = new String[tags.size()];
		        String[] tagNames = new String[101];
		        int contador = 0;
		        for(String s:tags) {
		        	
		        	if(contador <= 100) {
		        	System.out.println("Tags a buscar Producer: "+s);
		        	tagNames[contador] = s;
		        	contador = contador + 1;
		        	}
		        }
		        
		  
		// Establecemos el filtro
		twitterSource.setCustomEndpointInitializer(new ValidTagsTweetEndpoIntinitializer(tagNames));

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(PARALLELISM);

		// Añadimos la fuente y generamos el stream como la salida de las llamadas
		// asíncronas para salvar los datos en MongoDB
		DataStream<String> stream = env.addSource(twitterSource);

		Properties props = LoggingFactory.getCloudKarafkaCredentials();

		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration<String> config = FlinkKafkaProducer010
				.writeToKafkaWithTimestamps(stream, props.getProperty("CLOUDKARAFKA_TOPIC").trim(), new SimpleStringSchema(),
						props);
		config.setWriteTimestampToKafka(false);
		config.setLogFailuresOnly(false);
		config.setFlushOnCheckpoint(true);

		stream.print();

		env.execute("Twitter Streaming Producer");
	}
	
	

}
