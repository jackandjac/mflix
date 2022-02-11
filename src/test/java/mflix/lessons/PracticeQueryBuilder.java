package mflix.lessons;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

@SpringBootTest
public class PracticeQueryBuilder extends AbstractLesson{
    public PracticeQueryBuilder(){
        super();
    }
    @Test
    public void testContrastSingleFieldQuery(){
        Document onerousFilter = new Document("cast","Salma Hayek");
        Document actual = this.moviesCollection.find(onerousFilter).limit(1).iterator().tryNext();
        String expectedTitle = "Roadracers";
        int expectedYear = 1994;
        Assert.assertEquals(expectedTitle,actual.getString("title"));
        Assert.assertEquals(expectedYear,(int)actual.getInteger("year"));

        Bson queryFilter = eq("cast", "Salma Hayek");
        Document builderActual =moviesCollection.find(queryFilter).limit(1).iterator().tryNext();
        Assert.assertEquals(actual,builderActual);
    }
    @Test
    public void contrastArrayInQuery(){
        Document oldFilter = new Document("cast", new Document("$all", Arrays.asList("Salma Hayek", "Johnny Depp")) );
        List<Document> oldResults = new ArrayList<>();
        moviesCollection.find(oldFilter).into(oldResults);
        Assert.assertEquals(1,oldResults.size());

        Bson queryFilter = all("cast", "Salma Hayek","Johnny Depp");
        List<Document> results = new ArrayList<>();
        moviesCollection.find(queryFilter).into(results);
        Assert.assertEquals(oldResults,results);
    }
    @Test
    public void testMultiplePredicates(){
        Bson queryFilter = and(eq("cast", "Tom Hanks"),gte("year", 1990), lt("year", 2005), gte("metacritic",80) );
        List<Document> results = new ArrayList<>();
        moviesCollection.find(queryFilter).into(results);
        Assert.assertEquals(4, results.size());
        Set<String> titles = results.stream().map(k-> (String)k.getString("title")).collect(Collectors.toSet());
        Assert.assertTrue(titles.containsAll(Arrays.asList("Forrest Gump", "Toy Story", "Toy Story 2", "Saving Private Ryan") ));
    }
    @Test
    public void testProjectionBuilder(){
        Document oldFilter = new Document("cast","Salma Hayek");

        Document oldResult =moviesCollection.find(oldFilter).limit(1).projection(new Document("title",1).append("year",1)).iterator().tryNext();
        Assert.assertEquals(3,oldResult.keySet().size());
        Assert.assertTrue(oldResult.keySet().containsAll(Arrays.asList("_id","title","year") ));
        Bson queryFilter = eq("cast","Salma Hayek");
        Document result = moviesCollection.find(queryFilter).limit(1).projection(fields(include("title","year")) ).iterator().tryNext();
        Assert.assertEquals(oldResult,result);

        Document newResult = moviesCollection.find(queryFilter).limit(1).projection(fields(include("title","year"), exclude("_id"))).iterator().tryNext();
        Assert.assertEquals(2, newResult.keySet().size());
        Document noId = moviesCollection.find(queryFilter).limit(1).projection(fields(include("title","year"), excludeId())).iterator().tryNext();
        Assert.assertEquals(newResult, noId);

    }

}








