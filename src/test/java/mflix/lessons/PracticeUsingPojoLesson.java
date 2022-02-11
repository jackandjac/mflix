package mflix.lessons;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import mflix.lessons.utils.ActorBasic;
import mflix.lessons.utils.ActorCodec;
import mflix.lessons.utils.ActorWithStringId;
import mflix.lessons.utils.StringObjectIdCodec;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.PropertyModelBuilder;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.*;

@SpringBootTest
public class PracticeUsingPojoLesson extends AbstractLesson {
    private ObjectId actor1Id;
    private ObjectId actor2Id;

    public PracticeUsingPojoLesson(){
        super();
    }
    @Before
    public void setUp() throws Exception{
        MongoCollection<Document> actors = testDb.getCollection("actors");
        Document actorDocument1 = new Document();
        actorDocument1.append("name", "Bruce Campbell");
        actorDocument1.append("date_of_birth", new SimpleDateFormat("yyyy-MM-dd").parse("1958-06-22"));
        actorDocument1.append("num_movies",127);
        actorDocument1.append("awards", Collections.EMPTY_LIST);

        Document actorDocument2 = new Document();
        actorDocument2.append("name", "Natalie Portman");
        actorDocument2.append("date_of_birth", new SimpleDateFormat("yyyy-MM-dd").parse("1981-06-09"));
        actorDocument2.append("num_movies", 63);
        actorDocument2.append("awards", Collections.EMPTY_LIST);

        List<Document> listOfActors = new ArrayList<>();
        listOfActors.add(actorDocument1);
        listOfActors.add(actorDocument2);
        actors.insertMany(listOfActors);
        actor1Id = actorDocument1.getObjectId("_id");
        actor2Id = actorDocument2.getObjectId("_id");
    }
    @Test
    public void testUsingPojo(){
        CodecRegistry pojoCodecRegistry  = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoCollection<ActorBasic> actors= testDb.getCollection("actors", ActorBasic.class).withCodecRegistry(pojoCodecRegistry);
        Bson queryFilter = new Document("_id", actor1Id);
        ActorBasic pojoActor = actors.find(queryFilter).iterator().tryNext();
        Assert.assertNotNull(pojoActor);

        MongoCollection<Document> actorCollectionDoc = testDb.getCollection("actors");
        Document docActor = actorCollectionDoc.find(queryFilter).iterator().tryNext();


        System.out.println("[debug][pojo] " + pojoActor.getDateOfBirth());
        System.out.println("[Debug][document] " +  docActor.get("date_of_birth"));
        Assert.assertEquals(docActor.getObjectId("_id"),pojoActor.getId());
        Assert.assertEquals(docActor.getString("name"), pojoActor.getName());
        Assert.assertEquals(docActor.get("date_of_birth"), pojoActor.getDateOfBirth());
        Assert.assertEquals(docActor.get("awards"), pojoActor.getAwards());
        Assert.assertEquals(docActor.get("num_movies"),pojoActor.getNumMovies());
    }

    public ActorWithStringId fromDocument(Document doc){
        ActorWithStringId actor = new ActorWithStringId();
        actor.setId(doc.getObjectId("_id").toHexString());
        actor.setName(doc.getString("name"));
        actor.setNumMovies(doc.getInteger("num_movies"));
        actor.setDateOfBirth(doc.getDate("date_of_birth"));
        actor.setAwards((List<Document>)doc.get("awards"));

        return actor;
    }
    @Test
    public  void testMappingDocumentsToActorClass(){
        MongoCollection<Document> actors = testDb.getCollection("actors");
        Document actorDocument = actors.find(new Document("_id", actor1Id)).iterator().tryNext();

        Assert.assertNotNull("we should be able to find this actor", actorDocument);
        ActorWithStringId actor = fromDocument(actorDocument);
        Assert.assertNotNull(actor);

        Assert.assertEquals(actor.getId(), actorDocument.getObjectId("_id").toString());
        Assert.assertEquals(actor.getName(), actorDocument.getString("name"));
        Assert.assertEquals(actor.getDateOfBirth(), actorDocument.getDate("date_of_birth"));
        Assert.assertEquals(actor.getAwards(), actorDocument.get("awards"));
        Assert.assertEquals(actor.getNumMovies(), actorDocument.get("num_movies"));
    }
    @Test
    public void testReadObjectsWithCustomCodec(){
        ActorCodec actorCodec = new ActorCodec();
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromCodecs(actorCodec));
        Bson queryFilter = new Document("_id", actor1Id);
        MongoCollection<ActorWithStringId> customCodecActors = testDb.getCollection("actors",ActorWithStringId.class).withCodecRegistry(codecRegistry);
        ActorWithStringId actor = customCodecActors.find(queryFilter).first();
        Assert.assertEquals(actor1Id.toHexString(), actor.getId());

        MongoCollection<Document> actorsCollectionDoc = testDb.getCollection("actors");
        ActorWithStringId mappedActor = fromDocument(actorsCollectionDoc.find(queryFilter).iterator().tryNext());

        Assert.assertNotNull(actor);
        Assert.assertNotNull(mappedActor);
        Assert.assertEquals(mappedActor.getId(), actor.getId());
        Assert.assertEquals(mappedActor.getName(), actor.getName());
        Assert.assertEquals(mappedActor.getDateOfBirth(), actor.getDateOfBirth());
        Assert.assertEquals(mappedActor.getAwards(), actor.getAwards());
        Assert.assertEquals(mappedActor.getNumMovies(), actor.getNumMovies());
        // looks like they do, which is great news.

    }
    @Test
    public void testWriteObjectsWithCustomCodec(){
        ActorCodec codec = new ActorCodec();
        CodecRegistry codecRegistry =  fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromCodecs(codec));
        MongoCollection<ActorWithStringId> customCodecActors = testDb.getCollection("actors", ActorWithStringId.class).withCodecRegistry(codecRegistry);
        ActorWithStringId actorNew = new ActorWithStringId();
        actorNew.setNumMovies(2);
        actorNew.setName("Norberto");
        customCodecActors.insertOne(actorNew);
        Assert.assertNotNull(actorNew.getId());
    }
    @Test
    public void testReadObjectWithCustomFieldCodec(){
        ClassModelBuilder<ActorWithStringId> classModelBuilder = ClassModel.builder(ActorWithStringId.class);

        PropertyModelBuilder<String> idPropertyModelBuilder =(PropertyModelBuilder<String>) classModelBuilder.getProperty("id");
        idPropertyModelBuilder.codec(new StringObjectIdCodec());
        CodecRegistry stringIdCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().register(classModelBuilder.build()).automatic(true).build()));

        MongoCollection<ActorWithStringId> actors = testDb.getCollection("actors", ActorWithStringId.class).withCodecRegistry(stringIdCodecRegistry);
        Bson queryFilter = new Document("_id", actor1Id);
        ActorWithStringId stringIdActor = actors.find(queryFilter).iterator().tryNext();
        Assert.assertNotNull(stringIdActor);
        Assert.assertTrue(stringIdActor instanceof ActorWithStringId);
        Assert.assertEquals(actor1Id.toHexString() , stringIdActor.getId());

        MongoCollection<Document> actorsCollectionDoc = testDb.getCollection("actors");
        ActorWithStringId mappedActor = fromDocument(actorsCollectionDoc.find(queryFilter).iterator().tryNext() );

        Assert.assertNotNull(mappedActor);
        Assert.assertEquals(mappedActor.getId(), stringIdActor.getId());
        Assert.assertEquals(mappedActor.getName(), stringIdActor.getName());
        System.out.println("[debuginfo] " + mappedActor.getDateOfBirth());
        System.out.println("[Debug][stringIdActor] " +  stringIdActor.getDateOfBirth());

        Assert.assertEquals(mappedActor.getDateOfBirth(), stringIdActor.getDateOfBirth());
        Assert.assertEquals(mappedActor.getAwards(), stringIdActor.getAwards());
        Assert.assertEquals(mappedActor.getNumMovies(), stringIdActor.getNumMovies());
        // looks like they do, which is great news.
    }
    @After
    public void tearDown(){
        testDb.getCollection("actors").drop();
        actor1Id = null;
        actor2Id = null;
    }
}
