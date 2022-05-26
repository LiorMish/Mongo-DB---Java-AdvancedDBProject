/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

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
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{

	private MongoClient mongoClient;
	
	// connect to Mongo DB
	private DBCollection getMongoCollection(String collectionName) {
		try {
			mongoClient = new MongoClient( "localhost" , 27017 );
			return mongoClient.getDB("ProjectDB").getCollection(collectionName);
		}
		catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}

	// disconnect from Mongo DB
	private void closeMongo() {
		if (mongoClient!=null) 
			mongoClient.close();
		
	}


	private boolean isExistUser(String username){
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("UserName", username);
			DBCollection  dbCollection = getMongoCollection("Users");
			DBCursor dbCursor = dbCollection.find(query);
			if (dbCursor.size()>0) {
				result=true;
			}
			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
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

	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username") String username,
			@RequestParam("title") String title,
			HttpServletResponse response){
		System.out.println(username + " " + title);
		//:TODO your implementation
		HttpStatus status;
		int count=0;
		try {
			DBCollection  dbCollection = getMongoCollection("UsersHistory");
			BasicDBObject history = new BasicDBObject();
			history.put("UserName", username);
			history.put("Title", title);
			history.put("Timestamp", new Date().getTime() );
			if(isExistUser(username) && isExistsItem(title))
			{
				dbCollection.insert(history);
				count++;
			}		
			closeMongo();			
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			if(count >= 1) 
				status = HttpStatus.OK;
			else 
				status = HttpStatus.CONFLICT;
			
			response.setStatus(status.value());
		}
	}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity") String username){
		//:TODO your implementation
		ArrayList<HistoryPair> userHistoryPairList = new ArrayList<HistoryPair>();
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("UserName", username);
			DBCollection  dbCollection = getMongoCollection("UsersHistory");
			DBCursor dbCursor = dbCollection.find(query);
			BasicDBObject timestampSort = new BasicDBObject("Timestamp",-1);
			dbCursor.sort(timestampSort);
			while (dbCursor.hasNext()) 
			{ 
				DBObject nextObject = dbCursor.next();
				String credentials = (String) nextObject.get("Title");
				long getTimeStamp = (long) nextObject.get("Timestamp");
				HistoryPair toAdd = new HistoryPair(credentials,new Date(getTimeStamp));
				userHistoryPairList.add(toAdd);
			}
			mongoClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userHistoryPairList.toArray(new HistoryPair[userHistoryPairList.size()]);
	}


	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@SuppressWarnings("finally")
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
		ArrayList<HistoryPair> userHistoryPairList = new ArrayList<HistoryPair>();
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("Title", title);
			DBCollection  dbCollection = getMongoCollection("UsersHistory");
			DBCursor dbCursor = dbCollection.find(query);
			BasicDBObject timestampSort = new BasicDBObject("Timestamp",-1);
			dbCursor.sort(timestampSort);
			while (dbCursor.hasNext()) 
			{ 
				DBObject nextObject = dbCursor.next();
				String credentials = (String) nextObject.get("UserName");
				long getTimeStamp = (long) nextObject.get("Timestamp");
				HistoryPair toAdd = new HistoryPair(credentials,new Date(getTimeStamp));
				userHistoryPairList.add(toAdd);
			}
			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			return userHistoryPairList.toArray(new HistoryPair[userHistoryPairList.size()]);
		}
	}

	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@SuppressWarnings("finally")
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		ArrayList<User> usersList = new ArrayList<User>();
		HistoryPair[] usersHistory = null;
		try {
			usersHistory = getHistoryByItems(title);
			for (HistoryPair usersHistoryPair : usersHistory) {
				DBCollection  dbCollection = getMongoCollection("Users");
				BasicDBObject query = new BasicDBObject();
				query.put("UserName", usersHistoryPair.credentials);
				DBCursor dbCursor = dbCollection.find(query);
				if (dbCursor.hasNext()) 
				{ 
					DBObject nextObject = dbCursor.next();
					String userName = (String) nextObject.get("UserName");
					String firstName = (String) nextObject.get("FirstName");
					String lastName = (String) nextObject.get("LastName");
					User newUser = new User(userName, firstName, lastName);
					usersList.add(newUser);
				}
				closeMongo();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			return usersList.toArray(new User[usersList.size()]);
		}
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		//:TODO your implementation
		double Similarity=0;
		try {
			Set<String> titleUsersSet1 = new HashSet<String>();
			Set<String> titleUsersSet2 = new HashSet<String>();
			for (HistoryPair historyPair : getHistoryByItems(title1)) {
				titleUsersSet1.add(historyPair.credentials);
			}	
			for (HistoryPair historyPair : getHistoryByItems(title2)) {
				titleUsersSet2.add(historyPair.credentials);
			}	
			
			// union of 2 sets
			Set<String> union =  new HashSet<String>(titleUsersSet1);
			union.addAll(titleUsersSet2);
			
			// intersection of 2 sets
			Set<String> intersection = new HashSet<String>(titleUsersSet1);
			intersection.retainAll(titleUsersSet2);


			if(union.size() == 0 ) {
				return Similarity;
			}

			Similarity = ((double)intersection.size()) / union.size();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return Similarity;
	}


}
