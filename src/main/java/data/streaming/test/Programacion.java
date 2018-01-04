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
				
				
				// Generamos el batch
				List<Document> patentsDocuments = Utils.generaBatch();

				// Generamos los ratings
				Utils.generaRatings(patentsDocuments);
				

				// Sistema de recomendacion
				Utils.sistemaRecomendacion();
				
				//Vistas optimizadas
				
				Utils.vistasOptimizadas();
				
				//Diferencia entre patentes viejas y nuevas
				
				Utils.interseccionPatentes();
				
				System.out.println("Terminado");
				
				
				
			}
		
		};
		final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 10, 60*60*24, SECONDS);
		/*
		 * scheduler.schedule(new Runnable() { public void run() {
		 * beeperHandle.cancel(true); } }, 999999999 * 99999999, SECONDS);
		 */
	}

}
