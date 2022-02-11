package mflix.lessons;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
public class PracticeUsingAggregationBuilder extends AbstractLesson {
    @Test
    public void singleStageAggregation(){
        String country = "Portugal";
        Bson countryPT = Filters.eq("countries", country);
        List<Bson> pipeline = new ArrayList<>();
        Bson matchStage = Aggregates.match(countryPT);
        pipeline.add(matchStage);

        AggregateIterable<Document>  iterable = moviesCollection.aggregate(pipeline);
        List<Document> builderMatchStageResults = new ArrayList<>();
        iterable.into(builderMatchStageResults);
        Assert.assertEquals(115, builderMatchStageResults.size());
    }
    @Test
    public void aggregateServalStages(){

        List<Bson> pipeline = new ArrayList<>();
        String country = "Portugal";
        Bson countryPt = Filters.eq("countries", country);
        Bson matchStage = Aggregates.match(countryPt);

        Bson unwindStage = Aggregates.unwind("$cast");

        String groupIdCast = "$cast";

        BsonField sum1 = Accumulators.sum("count",1);
        Bson groupStage = Aggregates.group(groupIdCast,sum1);

        Bson sortOrder = Sorts.descending("count");
        Bson sortStage = Aggregates.sort(sortOrder);

        pipeline.add(matchStage);
        pipeline.add(unwindStage);
        pipeline.add(groupStage);
        pipeline.add(sortStage);

        AggregateIterable<Document> iterable = moviesCollection.aggregate(pipeline);
        List<Document> groupByResults = new ArrayList<>();
        iterable.into(groupByResults);

        List<Bson> shortPipeline = new ArrayList<>();
        shortPipeline.add(matchStage);
        shortPipeline.add(unwindStage);

        Bson sortByCountStage = Aggregates.sortByCount("$cast");
        shortPipeline.add(sortByCountStage);

        List<Document> shortResults = new ArrayList<>();
        AggregateIterable<Document> siterable = moviesCollection.aggregate(shortPipeline);
        siterable.into(shortResults);

        Assert.assertEquals(groupByResults, shortResults);

    }
    @Test
    public void complexStages(){
        List<Bson> pipeline = new ArrayList<>();
        Bson unwindCast = Aggregates.unwind("$cast");
        Bson groupCastSet = Aggregates.group("",Accumulators.addToSet("cast_list", "$cast"));
        Facet castMembersFacet  = new Facet("cast_member", unwindCast, groupCastSet);

        Bson unwindstageGenres = Aggregates.unwind("$genres");
        Facet genresCountFacet = new Facet("genres_count", unwindstageGenres, Aggregates.sortByCount("$genres"));

        Bson yearBucketStage = Aggregates.bucketAuto("$year",10);
        Facet yearBucketFacet = new Facet("year_bucket", yearBucketStage);
        Bson facetsStage = Aggregates.facet(castMembersFacet,genresCountFacet, yearBucketFacet);

        Bson matchStage = Aggregates.match(Filters.eq("countries", "Portugal"));

        pipeline.add(matchStage);
        pipeline.add(facetsStage);

        AggregateIterable<Document> iterable = moviesCollection.aggregate(pipeline);
        List<Document> results = new ArrayList<>();
        iterable.into(results);
        for (Document item:results) {
            System.out.println(item);
        }
        Assert.assertEquals(1,results.size());
    }
}











