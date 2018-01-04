package data.streaming.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.ItemScorer;
import org.grouplens.lenskit.Recommender;
import org.grouplens.lenskit.RecommenderBuildException;
import org.grouplens.lenskit.core.LenskitConfiguration;
import org.grouplens.lenskit.core.LenskitRecommender;
import org.grouplens.lenskit.data.dao.EventCollectionDAO;
import org.grouplens.lenskit.data.dao.EventDAO;
import org.grouplens.lenskit.data.event.Event;
import org.grouplens.lenskit.data.event.MutableRating;
import org.grouplens.lenskit.knn.user.UserUserItemScorer;
import org.grouplens.lenskit.scored.ScoredId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordDTO;
import data.streaming.dto.KeywordFecha;
import data.streaming.dto.Patent;
import data.streaming.dto.PatentFull;
import data.streaming.dto.PatentePatenteRating;
import data.streaming.dto.Recomendation;
import data.streaming.dto.TweetDTO;

public class Utils {

	// public static final String[] TAGNAMES = { "#OTDirecto8D", "#InmaculadaConcepcion" };
	//public static final String[] TAGNAMES = { "Testing"};
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final int MAX_RECOMMENDATIONS = 3;

	public static TweetDTO createTweetDTO(String x) {
		TweetDTO result = null;

		try {
			result = mapper.readValue(x, TweetDTO.class);
		} catch (IOException e) {

		}
		return result;
	}

	public static boolean esValido(String x) {
		boolean result = true;

		if (createTweetDTO(x) == null) {
			return false;
		}
		return result;
	}

	public static TweetDTO insertaBD(TweetDTO t) {

		// Creating a Mongo client
		MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());

		// Retrieving a collection

		MongoCollection<Document> batch = db.getCollection("tweets");

		// Document docu = new Document().append("creationDate",
		// t.getCreatedAt()).append("language", t.getLanguage()).append("text",
		// t.getText()).append("user", t.getUser());
		Document userData = new Document().append("idStr", t.getUser().getIdStr()).append("name", t.getUser().getName())
				.append("screenName", t.getUser().getScreenName()).append("friends", t.getUser().getFriends())
				.append("followers", t.getUser().getFollowers());
		Document tweet = new Document().append("creationDate", t.getCreatedAt()).append("language", t.getLanguage())
				.append("text", t.getText()).append("userData", userData);
		batch.insertOne(tweet);

		client.close();

		return t;

	}
	

	public static String parseaFecha(String fecha) {

		//Parseamos la fecha
		String[] split = fecha.split(" ");
		
		String mes = split[1];
		String dia = split[2];
		String anyo = split[5];
		
		
		
		String concat = dia + "/" + mes + "/" + anyo;
		
		
		String fechaFormateada = "";
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy",Locale.ENGLISH);
		try {
		    Date varDate=dateFormat.parse(concat);
		    dateFormat=new SimpleDateFormat("dd-MM-yyyy");
		    
		    fechaFormateada = dateFormat.format(varDate);
		    //System.out.println("Date :"+dateFormat.format(varDate));
		    
			
		}catch (Exception e) {
		    // TODO: handle exception
		    e.printStackTrace();
		}
		return fechaFormateada;
	}
	
	public static ItemRecommender getRecommender(Set<PatentePatenteRating> dtos) throws RecommenderBuildException {
		LenskitConfiguration config = new LenskitConfiguration();
		EventDAO myDAO = EventCollectionDAO.create(createEventCollection(dtos));

		config.bind(EventDAO.class).to(myDAO);
		config.bind(ItemScorer.class).to(UserUserItemScorer.class);
		// config.bind(BaselineScorer.class,
		// ItemScorer.class).to(UserMeanItemScorer.class);
		// config.bind(UserMeanBaseline.class,
		// ItemScorer.class).to(ItemMeanRatingItemScorer.class);

		Recommender rec = LenskitRecommender.build(config);
		return rec.getItemRecommender();
	}

	private static Collection<? extends Event> createEventCollection(Set<PatentePatenteRating> ratings) {
		List<Event> result = new LinkedList<>();

		for (PatentePatenteRating dto : ratings) {
			MutableRating r = new MutableRating();
			r.setItemId(dto.getPatent1().hashCode());
			r.setUserId(dto.getPatent2().hashCode());
			r.setRating(dto.getRating());
			result.add(r);
		}
		return result;
	}
	
	public static List<Recomendation> saveModel(ItemRecommender irec, Set<PatentePatenteRating> set) throws IOException {
		Map<String, Long> keys = Maps.asMap(set.stream().map((PatentePatenteRating x) -> x.getPatent1()).collect(Collectors.toSet()),
				(String y) -> new Long(y.hashCode()));
		Map<Long, List<String>> reverse = set.stream().map((PatentePatenteRating x) -> x.getPatent1())
				.collect(Collectors.groupingBy((String x) -> new Long(x.hashCode())));

		List<Recomendation> result = new ArrayList<Recomendation>(); 
		
		for (String key : keys.keySet()) {
			List<ScoredId> recommendations = irec.recommend(keys.get(key), MAX_RECOMMENDATIONS);
			List<String> tmp = new ArrayList<String>();
			
			if (recommendations.size() > 0) {
				
				for( ScoredId rc:recommendations) {
					tmp.add(reverse.get(rc.getId()).get(0));
				}
				
				result.add(new Recomendation(key, tmp));
			}
		}
		
		return result;
	}
	
	
	public static void interseccionPatentes() {

		// Creating a Mongo client
		MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");
		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());

		MongoCollection<Document> patentsAux = db.getCollection("patentsAux");
		
		MongoCollection<Document> patents = db.getCollection("patents");
		
		
		Set<Document> patsAux = (Set<Document>) patentsAux.find().into(new HashSet<Document>());
		
		Set<Document> pats = (Set<Document>) patents.find().into(new HashSet<Document>());
		
		
		Set<PatentFull> patsAuxSet = new HashSet<PatentFull>();
		Set<PatentFull> patsSet = new HashSet<PatentFull>();
		
		Set<PatentFull> newPatents = new HashSet<PatentFull>();
		List<Document> listNewPatents = new ArrayList<Document>();
		
		
		for(Document d: patsAux) {
			String idPatent = (String) d.get("idPatent");
			String title = (String) d.get("title");
			String date = (String) d.get("date");
			List<String> inventors = (List<String>) d.get("inventors");
			List<String> keywords = (List<String>) d.get("keywords");
			
			PatentFull ptf = new PatentFull(title,date,idPatent,inventors,keywords);
			
			patsAuxSet.add(ptf);
			
		}
		
		for(Document d: pats) {
			String idPatent = (String) d.get("idPatent");
			String title = (String) d.get("title");
			String date = (String) d.get("date");
			List<String> inventors = (List<String>) d.get("inventors");
			List<String> keywords = (List<String>) d.get("keywords");
			
			PatentFull ptf = new PatentFull(title,date,idPatent,inventors,keywords);
			
			patsSet.add(ptf);
			
		}
		
		newPatents = Utils.difference(patsAuxSet, patsSet);
		
		
		for(PatentFull ptf: newPatents) {
			String idPatent = ptf.getIdPatent();
			String title = ptf.getTitle();
			String date = ptf.getDate();
			List<String> inventors = ptf.getInventors();
			List<String> keywords = ptf.getKeywords();
			
			Document docu = new Document().append("idPatent",idPatent).append("title",title).append("date",date).append("inventors",inventors).append("keywords",keywords);
			
			listNewPatents.add(docu);
			
		}
		
		
		
		Bson filter4 = new Document();
		patentsAux.deleteMany(filter4);
		System.out.println("------------------------------------------------------");
		System.out.println("Insertamos la diferencia de las patentes");

		patentsAux.insertMany(listNewPatents);
		client.close();

	}
	
	
	public static void sistemaRecomendacion() {

		// Creating a Mongo client
		MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());

		MongoCollection<Document> ratings = db.getCollection("ratings");

		MongoCollection<Document> recomendationsBd = db.getCollection("recomendations");

		List<Document> ratingDocuments2 = (List<Document>) ratings.find().into(new ArrayList<Document>());

		Set<PatentePatenteRating> ratings2 = new HashSet<PatentePatenteRating>();

		for (Document d : ratingDocuments2) {
			String patent1Str = (String) d.get("patent1");
			String patent2Str = (String) d.get("patent2");
			Integer ratingInt = (Integer) d.get("rating");

			ratings2.add(new PatentePatenteRating(patent1Str, patent2Str, ratingInt));
		}

		try {
			// Esta linea se refiere a coger de nuestra BD los ratings
			Set<PatentePatenteRating> set = ratings2;

			ItemRecommender irec = Utils.getRecommender(set);
			List<Recomendation> recomendations = new ArrayList<Recomendation>();
			// Exportar los datos a una coleccion nueva
			recomendations = Utils.saveModel(irec, set);

			// Guardamos las recomendaciones

			Bson filter3 = new Document();
			recomendationsBd.deleteMany(filter3);

			List<Document> documentRecomendations = new ArrayList<Document>();
			for (Recomendation rec : recomendations) {

				Document docu = new Document().append("patent", rec.getPatent()).append("recomendations",
						rec.getRecomendations());
				documentRecomendations.add(docu);

			}

			System.out.println("------------------------------------------------------");
			System.out.println("Insertamos las recomendaciones");
			System.out.println("Tamaño de las recomendaciones: " + documentRecomendations.size());

			recomendationsBd.insertMany(documentRecomendations);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (RecommenderBuildException e) {
			e.printStackTrace();
		}

		client.close();

	}
	
	
	public static List<Document> generaBatch() {
		

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

		
		List<Document> tweetsDocuments = (List<Document>) tweets.find().into(new ArrayList<Document>());
		
		List<Document> tweetsContainsKeyword = new ArrayList<Document>();
		
		Integer contador2 = 0;
		for (String s : tagNames) {
			// System.out.println("Tags actual: " + s);
			//BasicDBObject regexQuery = new BasicDBObject();
			//regexQuery.put("text", new BasicDBObject("$regex", s).append("$options", "i"));
			
			for (Document d : tweetsDocuments) {
				
				if(d.getString("text").toLowerCase().contains(s.toLowerCase())) {
					tweetsContainsKeyword.add(d);
				}
				
				
			}
			
			//List<Document> tweetsDocuments = (List<Document>) tweets.find(regexQuery).into(new ArrayList<Document>());

			List<String> creationDates = new ArrayList<String>();

			// Cogemos las fechas de todos los tweets
			for (Document doc : tweetsContainsKeyword) {
				String fechaCreacion = doc.getString("creationDate");
				creationDates.add(fechaCreacion);

			}

			for (String fecha : creationDates) {
				Integer numTweets = 0;

				String fechaFormateada = Utils.parseaFecha(fecha);
				// System.out.println("Fecha actual: " + fechaFormateada);

				for (Document docu : tweetsContainsKeyword) {
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
				tweetsContainsKeyword.clear();

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
			String fechaCreacion = d.getString("date");
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
		
		return patentsDocuments;

	}
	
	
	

	public static void generaRatings(List<Document> patentsDocuments) {
		
		
		
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
		
		// Creating a Mongo client
		MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());

		MongoCollection<Document> ratings = db.getCollection("ratings");

		Bson filter2 = new Document();
		ratings.deleteMany(filter2);
		System.out.println("------------------------------------------------------");
		System.out.println("Insertamos los ratings");
		System.out.println("Tamaño de los ratings: " + ratingsDocuments.size());
		ratings.insertMany(ratingsDocuments);

		client.close();
		

	}
	
	
	public static void vistasOptimizadas() {
		
		 // Primer grafico
		
		
		String url = "https://si1718-rrv-patents.herokuapp.com/api/v1/patents/";
		
		List<Patent> patents = new ArrayList<Patent>();
		List<Integer> years = new ArrayList<Integer>();
		List<String> consecutiveYears = new ArrayList<String>();
		List<Integer> patentsValuePerYear = new ArrayList<Integer>();
		
		List<String> idGroups = new ArrayList<String>();
        List<Integer> numInventorsPerGroup = new ArrayList<Integer>();
		try {
			URL urla = new URL(url);
			 
		    // read from the URL
		    Scanner scan = new Scanner(urla.openStream());
		    String str = new String();
		    while (scan.hasNext())
		        str += scan.nextLine();
		    scan.close();
		 
		    JSONArray jsonarray = new JSONArray(str);
		    for (int i = 0; i < jsonarray.length(); i++) {
		        JSONObject jsonobject = jsonarray.getJSONObject(i);
		        String title = jsonobject.getString("title");
		        String date = jsonobject.getString("date");
		        String idPatent = jsonobject.getString("idPatent");
		        
		        Patent patent = new Patent(title,date,idPatent);
		        patents.add(patent);
		        
		    }
			
		    // Recorremos las patentes para sacar los años    
            for(Patent p: patents) {
                
                    String year = p.getDate().split("-")[0];
                    Integer yearNumber = new Integer(year);
                    years.add(yearNumber);
            }
		    
         // Ordenamos el array para coger el minimo y maximo
            Collections.sort(years);
            
            Integer startYear= years.get(0);
            Integer finishYear = years.get(years.size()-1);
            
            
            
            //Generamos los años que queremos que tenga nuestro diagrama de barras
            for(Integer i = startYear ;i <= finishYear; i++) {
                
                consecutiveYears.add(String.valueOf(i));
            }
            
          //Generamos el numero de patentes para cada año
            
            Integer numPatentsPerYear = 0;
            
            Integer cont = startYear;
            
            while (cont <= finishYear) {
                
                for(Patent p: patents) {
                    String year = p.getDate().split("-")[0];
                    Integer yearNumber =  new Integer(year);
                    if(cont.equals(yearNumber)){
                        numPatentsPerYear = numPatentsPerYear +1;
                    }
                }
                patentsValuePerYear.add(numPatentsPerYear);
                numPatentsPerYear = 0;
                cont++;
                
            }
            
            // Segundo grafico
            
            String url2 = "https://si1718-rgg-groups.herokuapp.com/api/v1/groups";
            
           
            
            URL urlb = new URL(url2);
			 
		    // read from the URL
		    Scanner scan2 = new Scanner(urlb.openStream());
		    String str2 = new String();
		    while (scan2.hasNext())
		        str2 += scan2.nextLine();
		    scan2.close();
		 
		    JSONArray jsonarray2 = new JSONArray(str2);
		    for (int i = 0; i < jsonarray2.length(); i++) {
		        JSONObject jsonobject2 = jsonarray2.getJSONObject(i);
		        String idGroup = jsonobject2.getString("idGroup");
		        JSONArray components = jsonobject2.getJSONArray("components");
		        Integer size = components.length();
		        
		        idGroups.add(idGroup);
		        numInventorsPerGroup.add(size);
		        
		    }
		    
		    
			
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		

		// Creating a Mongo client
		MongoClientURI uri = new MongoClientURI("mongodb://rrv:rrv@ds255455.mlab.com:55455/si1718-rrv-patents");

		MongoClient client = new MongoClient(uri);

		MongoDatabase db = client.getDatabase(uri.getDatabase());

		MongoCollection<Document> optimizedViews = db.getCollection("optimizedViews");

		// Borramos el contenido
		Bson filter = new Document();
		optimizedViews.deleteMany(filter);
		
		Document doc = new Document().append("consecutiveYears", consecutiveYears).append("patentsValuePerYear", patentsValuePerYear).append("idGroups", idGroups).append("numInventorsPerGroup", numInventorsPerGroup);
		
		System.out.println("------------------------------------------------------");
		System.out.println("Insertamos la optimizacion de vistas");
		optimizedViews.insertOne(doc);
		
		client.close();

	}
	
	 public static <T> Set<T> difference(Set<T> setA, Set<T> setB) {
		    Set<T> tmp = new TreeSet<T>(setA);
		    tmp.removeAll(setB);
		    return tmp;
		  }
	

}
