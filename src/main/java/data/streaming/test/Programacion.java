package data.streaming.test;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordFecha;
import data.streaming.dto.PatentePatenteRating;
import data.streaming.utils.Utils;

public class Programacion {

	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	public void beepForAnHour() {
		final Runnable beeper = new Runnable() {
			public void run() {
				System.out.println("beep");

				Map<KeywordFecha, Integer> map = new HashMap<KeywordFecha, Integer>();
				Map<KeywordFecha, Integer> mapAntiguo = new HashMap<KeywordFecha, Integer>();
				
				

				// Creating a Mongo client
				MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

				MongoClient client = new MongoClient(uri);

				MongoDatabase db = client.getDatabase(uri.getDatabase());

				// Retrieving a collection

				MongoCollection<Document> patents = db.getCollection("patents");

				BasicDBObject regexQuery1 = new BasicDBObject();

				regexQuery1.put("keywords.0", new BasicDBObject("$exists", "true"));

				List<Document> patentsDocuments = (List<Document>) patents.find(regexQuery1)
						.into(new ArrayList<Document>());

				client.close();
				System.out.println("Nos traemos las patentes");

				Set<String> tags = new HashSet<String>();
				// Recorremos todos los keywords de las patentes
				for (Document document : patentsDocuments) {
					List<String> keywords = (List<String>) document.get("keywords");

					if (keywords.size() > 0) {
						for (String s : keywords) {
							tags.add(s);
						}
					}

				}

				// Generamos un array con los keywords

				String[] tagNames = new String[tags.size()];
				int contador = 0;
				for (String s : tags) {
					// System.out.println("Tags a buscar Batch: " + s);
					tagNames[contador] = s;
					contador = contador + 1;
				}

				// Iteramos los keywords

				client = new MongoClient(uri);

				db = client.getDatabase(uri.getDatabase());

				MongoCollection<Document> tweets = db.getCollection("tweets");

				System.out.println("Nos traemos los tweets");

				Integer contador2 = 0;
				for (String s : tagNames) {
					// System.out.println("Tags actual: " + s);
					BasicDBObject regexQuery = new BasicDBObject();
					regexQuery.put("text", new BasicDBObject("$regex", s).append("$options", "i"));

					List<Document> tweetsDocuments = (List<Document>) tweets.find(regexQuery)
							.into(new ArrayList<Document>());

					List<String> creationDates = new ArrayList<String>();

					// Cogemos las fechas de todos los tweets
					for (Document doc : tweetsDocuments) {
						String fechaCreacion = doc.getString("creationDate");
						creationDates.add(fechaCreacion);

					}

					for (String fecha : creationDates) {
						Integer numTweets = 0;

						String fechaFormateada = Utils.parseaFecha(fecha);
						// System.out.println("Fecha actual: " + fechaFormateada);

						for (Document docu : tweetsDocuments) {
							String fechaCreacionTweetFormateada = Utils.parseaFecha(docu.getString("creationDate"));
							
							if (fechaCreacionTweetFormateada.equals(fechaFormateada)) {
								numTweets = numTweets + 1;
							}

						}

						KeywordFecha kf = new KeywordFecha(s, fechaFormateada);

						// if the key hasn't been used yet,
						// we'll create a new ArrayList<String> object, add the value
						// and put it in the array list with the new key
						map.put(kf, numTweets);

					}

					System.out.println("------------------------------------------------------");
					System.out.println("Iteracion: " + contador2);
					System.out.println("Numero de tags: " + tags.size());
					System.out.println("Tag actual: " + s);
					contador2 = contador2 + 1;

				}

				client.close();

				List<Document> updatedBatch = new ArrayList<Document>();
				
				/*
				 * // Cogemos la fecha actual Date date = Calendar.getInstance().getTime();
				 * 
				 * // Display a date in day, month, year format DateFormat formatter = new
				 * SimpleDateFormat("dd/MM/yyyy"); String today = formatter.format(date);
				 */
				
				
				// Guardamos en la BD

				client = new MongoClient(uri);

				db = client.getDatabase(uri.getDatabase());

				MongoCollection<Document> batch = db.getCollection("batch");
				
				
				tweets = db.getCollection("tweets");

				
				
				
				//Actualizamos el contenido del batch
				
				List<Document> batchDocuments = (List<Document>) batch.find()
						.into(new ArrayList<Document>());
				
			
				//Metemos el batch antiguo en un map
				for (Document d : batchDocuments) {
					String keyword = d.getString("keyword");
					String fechaCreacion = d.getString("creationDate");
					Integer numTweets = d.getInteger("numTweets");
					KeywordFecha kf = new KeywordFecha(keyword, fechaCreacion);	
					mapAntiguo.put(kf, numTweets);
				}
				
				//Recorremos los nuevos
				for (Entry<KeywordFecha, Integer> entry : map.entrySet()){
					
					Integer numTweetsNuevos = entry.getValue();
					
					if(mapAntiguo.containsKey(entry.getKey())){
					    // if the key has already been used,
					    // we'll just grab the array list and add the value to it
						Integer numTweetsAntiguos = mapAntiguo.get(entry.getKey());
						
						mapAntiguo.put(entry.getKey(), numTweetsAntiguos + numTweetsNuevos);
					} else {
					    // if the key hasn't been used yet,
					    // we'll create a new ArrayList<String> object, add the value
					    // and put it in the array list with the new key
						
					    mapAntiguo.put(entry.getKey(), numTweetsNuevos);
					}
					
					
					
					
				}
				
				for (Map.Entry<KeywordFecha, Integer> entry : mapAntiguo.entrySet()) {

					Document docu = new Document().append("keyword", entry.getKey().getKeyword())
							.append("date", entry.getKey().getFecha()).append("numTweets", entry.getValue());
					updatedBatch.add(docu);
				}
				
			
				
				
				
				
				Bson filter = new Document();
				batch.deleteMany(filter);
				
				tweets.deleteMany(filter);

				System.out.println("------------------------------------------------------");
				System.out.println("Insertamos el batch");
				System.out.println("Tamaño del batch: " + updatedBatch.size());

				batch.insertMany(updatedBatch);

				client.close();

				// Generamos los ratings

				List<PatentePatenteRating> ratingsList = new ArrayList<PatentePatenteRating>();

				List<Document> ratingsDocuments = new ArrayList<Document>();
				for (int i = 0; i < patentsDocuments.size(); i++) {

					for (int j = 0; j < patentsDocuments.size(); j++) {
						Document patent1 = patentsDocuments.get(i);
						Document patent2 = patentsDocuments.get(j);

						// Generamos los ratings

						List<String> keywords1 = (List<String>) patent1.get("keywords");

						List<String> keywords2 = (List<String>) patent2.get("keywords");
						Double rating = 0.0;
						Double ratingNormalizado = 0.0;
						for (String s : keywords1) {

							if (keywords2.contains(s)) {
								rating = rating + 1.0;
							}

						}
						// Normalizamos el rating entre 1 y 5
						Double acierto = ((rating * 2) / ((keywords1.size() + keywords2.size()) * 1.0)) * 1.0;
						ratingNormalizado = acierto * 100.0;

						/*
						 * Long ratingNormalizadoInteger = (long)Math.floor(ratingNormalizado + 0.5d);
						 * 
						 * 
						 * 
						 * if(ratingNormalizadoInteger < 1) { ratingNormalizadoInteger = 1L; }
						 */
						//System.out.println("------------------------------------------------------");
						//System.out.println("Patente 1: " + i + " Patente 2: " + j);

						// Solo insertamos el rating si este es distinto de 0

						Integer ratingFinal = new Integer(ratingNormalizado.intValue());
						if (ratingFinal != 0) {
							String idPatente1 = (String) patent1.get("idPatent");
							String idPatente2 = (String) patent2.get("idPatent");
							ratingsList.add(new PatentePatenteRating(idPatente1, idPatente2, ratingFinal));
						}

					}

				}

				for (PatentePatenteRating ppr : ratingsList) {
					Document docu = new Document().append("patent1", ppr.getPatent1())
							.append("patent2", ppr.getPatent2()).append("rating", ppr.getRating());
					ratingsDocuments.add(docu);

				}
				client = new MongoClient(uri);

				db = client.getDatabase(uri.getDatabase());

				MongoCollection<Document> ratings = db.getCollection("ratings");

				Bson filter2 = new Document();
				ratings.deleteMany(filter2);
				System.out.println("------------------------------------------------------");
				System.out.println("Insertamos los ratings");
				System.out.println("Tamaño de los ratings: " + ratingsDocuments.size());
				ratings.insertMany(ratingsDocuments);

				client.close();

				// Sistema de recomendacion
				Utils.sistemaRecomendacion();
				
				
				//Diferencia entre patentes viejas y nuevas
				
				//Utils.interseccionPatentes();
				
				//Vistas optimizadas
				
				Utils.vistasOptimizadas();
				
				
				
				
				
			}
		
		};
		final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 10, 10, SECONDS);
		/*
		 * scheduler.schedule(new Runnable() { public void run() {
		 * beeperHandle.cancel(true); } }, 999999999 * 99999999, SECONDS);
		 */
	}

}
