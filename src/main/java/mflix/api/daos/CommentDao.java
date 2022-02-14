package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";
    private final Logger log;
    private MongoCollection<Comment> commentCollection;
    private CodecRegistry pojoCodecRegistry;
    private MongoCollection<Document> usersCollection;
    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        db.withReadConcern(ReadConcern.MAJORITY);

        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
        usersCollection = db.getCollection("users");
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        ObjectId obj_id = new ObjectId(id);
        // read concern demo
        //Comment comment = commentCollection.withReadConcern(ReadConcern.MAJORITY).find(new Document("_id", obj_id)).iterator().tryNext();
        return commentCollection.find(new Document("_id", obj_id)).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {

        // TODO> Ticket - Update User reviews: implement the functionality that enables adding a new
        // comment.
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        if(null == comment.getId()) throw new IncorrectDaoOperation("There are no comment id for this comment");

        String email = comment.getEmail();
        MongoCollection<Document> userCollection = db.getCollection("users");
        Bson queryFilter = Filters.eq("email",email);
        Document user =userCollection.find(queryFilter).iterator().tryNext();
        if(null == user ) throw new IncorrectDaoOperation("The user does not exist");
        try {
            commentCollection.insertOne(comment);
        }catch(MongoWriteException e){
            System.out.println(e.getStackTrace());
        }

        //write concern demo with timeout
        //commentCollection.withWriteConcern(WriteConcern.MAJORITY.withW(2500)).insertOne(comment);

        // write concern demo with 3 nodes acknowledgement
        // commentCollection.withWriteConcern(WriteConcern.ACKNOWLEDGED.withW(3)).insertOne(comment);

        Comment resultComment = commentCollection.find(Filters.eq("_id", new ObjectId(comment.getId()))).iterator().tryNext();

        return resultComment;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {

        // TODO> Ticket - Update User reviews: implement the functionality that enables updating an
        // user own comments
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        Document queryFilter = new Document("_id",new ObjectId(commentId)).append("email",email);
        UpdateResult updateResult =null;
        try {
           updateResult=commentCollection.updateOne(queryFilter, Updates.set("text", text) );
        }catch(MongoWriteException e){
            System.out.println(e.getStackTrace());
        }

        return (null != updateResult && updateResult.getModifiedCount() == 1) ;
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        // TODO> Ticket Delete Comments - Implement the method that enables the deletion of a user
        // comment
        // TIP: make sure to match only users that own the given commentId
        Bson queryFilter = Filters.and(Filters.eq("_id",new ObjectId(commentId)), Filters.eq("email",email));
        // TODO> Ticket Handling Errors - Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        DeleteResult dresult =null;
        try {
            dresult =commentCollection.deleteOne(queryFilter);
        }catch(MongoWriteException e){
            System.out.println(e.getStackTrace());
        }

         return (null!= dresult && dresult.getDeletedCount() ==1);
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();
        // // TODO> Ticket: User Report - execute a command that returns the
        // // list of 20 users, group by number of comments. Don't forget,
        // // this report is expected to be produced with an high durability
        // // guarantee for the returned documents. Once a commenter is in the
        // // top 20 of users, they become a Critic, so mostActive is composed of
        // // Critic objects.

        List<Bson> pipeline = new ArrayList<>();
        Bson fromStage = Aggregates.lookup("comments","email","email","comments");
        Bson addFieldStage = Aggregates.addFields(new Field<Document>("nums_comment", new Document("$size","$comments")) );
        Bson sortStage = Aggregates.sort(Sorts.descending("nums_comment"));
        Bson limitStage = Aggregates.limit(20);
        pipeline.add(fromStage);
        pipeline.add(addFieldStage);
        pipeline.add(sortStage);
        pipeline.add(limitStage);
        AggregateIterable<Document> docs =usersCollection.aggregate(pipeline);
        for(Document doc:docs){
            Critic critic = new Critic();
            critic.setId(doc.getString("email"));
            critic.setNumComments(doc.getInteger("nums_comment"));
            mostActive.add(critic);
        }
        return mostActive;
    }
}
