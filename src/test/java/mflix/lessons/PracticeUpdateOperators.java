package mflix.lessons;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static com.mongodb.client.model.Updates.*;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
public class PracticeUpdateOperators extends AbstractLesson{
    private ObjectId band1Id;
    private ObjectId band2Id;
    private Document bandOne;
    private Document bandTwo;

    public PracticeUpdateOperators(){
        super();
    }
    @Before
    public void setUP(){
        MongoCollection<Document> musicians = testDb.getCollection("artists");
        bandOne = new Document();
        bandOne.append("title","Gorillazz").append("num_albums",6).append("genre", "worldbeat").append("rating",8);
        bandTwo = new Document().append("title", "Weird Al Yankovic").append("num_albums",6).append("genre", "musical parodies").append("rating",8);

        List<Document> listOfMusicians = new ArrayList<>();
        listOfMusicians.add(bandOne);
        listOfMusicians.add(bandTwo);

        musicians.insertMany(listOfMusicians);
        band1Id = bandOne.getObjectId("_id");
        band2Id = bandTwo.getObjectId("_id");
    }
    @Test
    public void testReplaceDocument(){
        MongoCollection<Document> artists = testDb.getCollection("artists");

        Bson queryFilter = Filters.eq("_id", band1Id);
        Document myBand = artists.find(queryFilter).iterator().tryNext();
        Assert.assertEquals(bandOne,myBand);

        Document replaceBand = new Document("title", "Gorillaz");
        artists.replaceOne(queryFilter,replaceBand);

        Document newDoc = artists.find(queryFilter).iterator().tryNext();

        Assert.assertEquals(newDoc.getObjectId("_id"), band1Id);
        Assert.assertEquals(newDoc.get("title"),"Gorillaz");
        Assert.assertNull(newDoc.get("num_albums"));
        Assert.assertNull(newDoc.get("genre"));
        Assert.assertNull(newDoc.get("rating"));
    }
    @Test
    public void testSetFieldValueForOneDocument(){
        MongoCollection<Document> artists = testDb.getCollection("artists");
        Bson queryFilter = Filters.eq("_id",band1Id);
        Document wrongBandName = artists.find(queryFilter).iterator().tryNext();
        Assert.assertEquals(wrongBandName.get("title"),"Gorillazz");

        artists.updateOne(queryFilter,set("title","Gorillaz"));
        Document correctDoc = artists.find(queryFilter).iterator().tryNext();
        Assert.assertEquals(correctDoc.get("title"), "Gorillaz");
        Assert.assertNotNull(correctDoc.get("num_albums"));
        Assert.assertNotNull(correctDoc.get("genre"));
        Assert.assertNotNull(correctDoc.get("rating"));
    }

    @Test
    public void setFieldValueForManyDocument(){
        MongoCollection<Document> artists = testDb.getCollection("artists");
        artists.updateMany(Filters.eq("rating",8),set("rating",9));
        Document bandOneUpdate= artists.find(Filters.eq("_id",band1Id)).iterator().tryNext();
        Document bandTwoUpdate = artists.find(Filters.eq("_id", band2Id)).iterator().tryNext();

        Assert.assertEquals(bandOneUpdate.get("title"),"Gorillazz");
        Assert.assertEquals(bandOneUpdate.get("rating"),9);
        Assert.assertEquals(bandTwoUpdate.get("title"), "Weird Al Yankovic");
        Assert.assertEquals(bandTwoUpdate.get("rating"),9);

    }
    @Test
    public void testIncFieldValue(){
        MongoCollection<Document> artists = testDb.getCollection("artists");
        artists.updateMany(Filters.eq("rating",8),inc("rating",1));

        Document bandOneUpdated = artists.find(Filters.eq("_id",band1Id)).iterator().tryNext();
        Document bandTwoUpdated = artists.find(Filters.eq("_id",band2Id)).iterator().tryNext();

        Assert.assertEquals(bandOneUpdated.get("rating"),9);
        Assert.assertEquals(bandTwoUpdated.get("rating"),9);
    }
    @Test
    public void testUnsetFieldValue(){
        MongoCollection<Document> artists = testDb.getCollection("artists");
        artists.updateMany(Filters.exists("rating"),unset("rating"));
        Document bandOneUpdated = artists.find(Filters.eq("_id",band1Id)).iterator().tryNext();
        Document bandTwoUpdated = artists.find(Filters.eq("_id", band2Id)).iterator().tryNext();

        Assert.assertNotNull(bandOneUpdated.get("num_albums"));
        Assert.assertNotNull(bandTwoUpdated.get("num_albums"));
        Assert.assertNull(bandOneUpdated.get("rating"));
        Assert.assertNull(bandTwoUpdated.get("rating"));
    }



    @After
    public void tearDown() {
        testDb.getCollection("artists").drop();
        band1Id = null;
        band2Id = null;
    }
}











