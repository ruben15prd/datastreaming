package data.streaming.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.shaded.com.google.common.collect.Maps;
import org.bson.Document;
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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordDTO;
import data.streaming.dto.KeywordFecha;
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
	
	

}
