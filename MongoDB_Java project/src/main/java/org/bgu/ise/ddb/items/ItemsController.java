/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;


import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;



/**
 * @author Kathy & Lior
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	private Connection connection =null;
	private String username="mishutin";
	private String password="abcd";
	private String connectionUrl="jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
	private final String driver="oracle.jdbc.driver.OracleDriver";

	private MongoClient mongoClient;
	
	// connect to Mongo DB
	private DBCollection getMongoCollection(String collectionName) {
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			return mongoClient.getDB("ProjectDB").getCollection(collectionName);
		}
		catch (Exception e){
			System.out.println(e);
			return null;
		}
	}
	
	// disconnect from Mongo DB
	private void closeMongo() {
		if (mongoClient!=null) {
			mongoClient.close();
		}
	}

	
	private boolean isExistsItem(String title) {
		boolean result = false;
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("Title", title);
			DBCollection  dbCollection = getMongoCollection("MediaItems");
			DBCursor dbCursor = dbCollection.find(query);
			if (dbCursor.size()>0) {
				result=true;
			}
			closeMongo();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		return result;

	}
	
	private void addItem(MediaItems mediaItem) {
		try {
			BasicDBObject newItem = new BasicDBObject();
			newItem.put("Title", mediaItem.getTitle());
			newItem.put("Year", mediaItem.getProdYear());
			DBCollection  dbCollection = getMongoCollection("MediaItems");
			dbCollection.insert(newItem);
			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void deleteMediaItemsCollction() {
		try {
			DBCollection  dbCollection = getMongoCollection("MediaItems");
			dbCollection.drop();
			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
		deleteMediaItemsCollction();
		
		ResultSet resultSet =null;
		PreparedStatement preparedStatement =null;
		List<MediaItems> mediaItemsList = new ArrayList<MediaItems>();
		try {
			Class.forName(driver); 
			connection = DriverManager.getConnection(connectionUrl, username, password);
			connection.setAutoCommit(false);
			String query = "SELECT title,prod_year FROM MediaItems";
			preparedStatement = connection.prepareStatement(query);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				MediaItems newMediaItem = new MediaItems(resultSet.getString(1), resultSet.getInt(2));
				mediaItemsList.add(newMediaItem);
				if(!isExistsItem(newMediaItem.getTitle())) 
					addItem(newMediaItem);
				
			}
			resultSet.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		deleteMediaItemsCollction();
		URL url = new URL(urladdress);
		BufferedReader bufeerReader = null;
		String line = "";

		try {
			bufeerReader = new BufferedReader(new BufferedReader(new InputStreamReader(url.openStream())));
			while ((line = bufeerReader.readLine()) != null) {
				String[] values = line.split(",");
				String title= values[0];
				String year=values[1];
				
				if(!isExistsItem(title))
				{
					int yearInt = Integer.parseInt(year);
					MediaItems newItem = new MediaItems(title,yearInt);
					addItem(newItem);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	// URL: https://drive.google.com/uc?export=download&id=1ODmo3kU1mnu_4mTnf0kZP2LmekZEW9-D
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		ArrayList<MediaItems> mediaItemsList = new ArrayList<MediaItems>();
		if(topN <= 0) 
			return new MediaItems[0];
		else {
			try {
				DBCollection  dbCollection = getMongoCollection("MediaItems");
				DBCursor dbCursor = dbCollection.find().limit(topN);
				while (dbCursor.hasNext()) 
				{ 
					DBObject objectDB = dbCursor.next();
					String title = (String) objectDB.get("Title");
					int year = (int) objectDB.get("Year");
					mediaItemsList.add(new MediaItems(title ,year));

				}
				closeMongo();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return mediaItemsList.toArray(new MediaItems[mediaItemsList.size()]);
		
	}
		

}
