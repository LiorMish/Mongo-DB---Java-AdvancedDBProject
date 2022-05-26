/**
 * 
 */
package org.bgu.ise.ddb.registration;



import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import java.security.MessageDigest;


import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;

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
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	private MongoClient mongoClient;

	// connect to Mongo DB
	@SuppressWarnings("deprecation")
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

	// make user's password encoded
	private String hashPassword(String password) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
			messageDigest.update(password.getBytes());
			return new String(messageDigest.digest());
		}
		catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}





	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		//:TODO your implementation
		try {
			if (isExistUser(username)) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}else
			{
				BasicDBObject newUser = new BasicDBObject();
				newUser.put("UserName", username);
				newUser.put("FirstName", firstName);
				newUser.put("LastName", lastName);
				newUser.put("Password", hashPassword(password));
				newUser.put("RegistrationDate", new Date());
				DBCollection  dbCollection = getMongoCollection("Users");
				dbCollection.insert(newUser);
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
				closeMongo();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("UserName", username);
			DBCollection  dbCollection = getMongoCollection("Users");
			DBCursor dbCursor = dbCollection.find(query);
			if (dbCursor.size()>0) 
				result=true;

			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}

	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try {
			DBCollection  dbCollection = getMongoCollection("Users");
			BasicDBObject query = new BasicDBObject();
			query.put("UserName", username);
			query.put("Password", hashPassword(password));
			DBCursor dbCursor = dbCollection.find(query);
			if (dbCursor.size()>0) 
				result=true;

			closeMongo();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		try {
			DBCollection  dbCollection = getMongoCollection("Users");
			DBCursor dbCursor = dbCollection.find();
			Date currentDate = new Date();
			// 1 Day = 24*60*60*1000 = 864000000 milliseconds in a day
			int milliSeconds = 864000000;
			Date targetDate = new Date(currentDate.getTime() - (milliSeconds+1) * days);
			while (dbCursor.hasNext()) 
			{ 
				Date registrationDate = (Date) dbCursor.next().get("RegistrationDate");
				if(registrationDate.getTime() > targetDate.getTime()) 
					result++;			
			}
			closeMongo();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return result;

	}

	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		ArrayList<User> users = new ArrayList<User>();
		try {
			DBCollection  dbCollection = getMongoCollection("Users");
			DBCursor dbCursor = dbCollection.find();
			while (dbCursor.hasNext()) 
			{ 
				DBObject nextObject = dbCursor.next();
				String userName = (String) nextObject.get("UserName");
				String firstName = (String) nextObject.get("FirstName");
				String lastName = (String) nextObject.get("LastName");
				String password = (String) nextObject.get("Password");
				users.add(new User(userName, password ,firstName, lastName));
			}
			closeMongo();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return users.toArray(new User[users.size()]);


	}

}
