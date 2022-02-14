package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.text.MessageFormat;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Document> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //TODO> Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions");
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
            Bson queryFilter = Filters.eq("email",user.getEmail());
            User usr = usersCollection.find(queryFilter).iterator().tryNext();
            if(usr != null){
                throw new IncorrectDaoOperation("user already exists");
            }else {
                usersCollection.insertOne(user);
            }
        return true;
        //TODO > Ticket: Handling Errors - make sure to only add new users
        // and not users that already exist.

    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //TODO> Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
        Document session = new Document("user_id", userId).append("jwt",jwt);
        sessionsCollection.updateOne(Filters.eq("user_id",userId),Updates.set("jwt",jwt),new UpdateOptions().upsert(true));
        Document sess = sessionsCollection.find(Filters.eq("user_id",userId)).iterator().tryNext();
        return sess.get("jwt").equals(jwt);

    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;
        //TODO> Ticket: User Management - implement the query that returns the first User object.
        Bson emailPt = Filters.eq("email", email);
        user =usersCollection.find(emailPt).iterator().tryNext();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //TODO> Ticket: User Management - implement the method that returns Sessions for a given
        // userId
        Bson userIdPt = Filters.eq("user_id", userId);
        Document sess = sessionsCollection.find(userIdPt).iterator().tryNext();
        return sess!=null ? fromSessionDocument(sess):null;
    }

    public boolean deleteUserSessions(String userId) {
        //TODO> Ticket: User Management - implement the delete user sessions method
        Bson userPt = Filters.eq("user_id",userId);
        DeleteResult dresult = sessionsCollection.deleteOne(userPt);
        return (dresult.getDeletedCount() == 1);
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //TODO> Ticket: User Management - implement the delete user method
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions.
        Bson emailPt = Filters.eq("email",email);
        boolean delSession = deleteUserSessions(email);
        DeleteResult dresult = usersCollection.deleteOne(emailPt);
        return dresult.getDeletedCount() ==1 && delSession;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
        // be updated.
        //TODO > Ticket: Handling Errors - make this method more robust by
        // handling potential exceptions when updating an entry.
        if(null == userPreferences) throw new IncorrectDaoOperation("user preferences cannot be null");
        usersCollection.updateOne(Filters.eq("email",email),Updates.set("preferences",userPreferences));
        User userUpdated = usersCollection.find(Filters.eq("email",email)).iterator().tryNext();
        return userUpdated.getPreferences().keySet().size() == userPreferences.keySet().size();
    }
    private Session fromSessionDocument(Document sessionDoc){
        Session session = new Session();
        session.setUserId(sessionDoc.getString("user_id"));
        session.setJwt(sessionDoc.getString("jwt"));
        return session;
    }
}
