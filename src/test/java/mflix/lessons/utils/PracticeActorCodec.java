package mflix.lessons.utils;

import org.bson.*;
import org.bson.codecs.*;

import java.util.Date;
import java.util.List;

public class PracticeActorCodec implements CollectibleCodec<ActorWithStringId>
{
    private final Codec<Document> documentCodec;

    public PracticeActorCodec(){
        super();
        documentCodec = new DocumentCodec();
    }
    @Override
    public ActorWithStringId generateIdIfAbsentFromDocument(ActorWithStringId document) {
        return !documentHasId(document)? document.withNewId() : document ;
    }

    @Override
    public boolean documentHasId(ActorWithStringId document) {
        return (null!=document.getId());
    }

    @Override
    public BsonValue getDocumentId(ActorWithStringId document) {
        if(!documentHasId(document)){
            throw new IllegalStateException("This document does not have an _id");
        }
        return new BsonString(document.getId());
    }

    @Override
    public ActorWithStringId decode(BsonReader reader, DecoderContext decoderContext) {
        Document actorDoc = documentCodec.decode(reader,decoderContext);
        ActorWithStringId actor = new ActorWithStringId();
        actor.setId(actorDoc.getObjectId("_id").toHexString());
        actor.setName(actorDoc.getString("name"));
        actor.setDateOfBirth(actorDoc.getDate("date_of_birth"));
        actor.setAwards((List<Document>)actorDoc.get("awards"));
        actor.setNumMovies(actorDoc.getInteger("num_movies"));
        return actor;
    }

    @Override
    public void encode(BsonWriter writer, ActorWithStringId actor, EncoderContext encoderContext) {
        Document actorDoc = new Document();
        String actorId = actor.getId();
        String name = actor.getName();
        Date dateOfBirth = actor.getDateOfBirth();
        List awards = actor.getAwards();
        int numMovies = actor.getNumMovies();

        if(null != actorId){
            actorDoc.put("_id",actorId);
        }
        if(null != name){
            actorDoc.put("name", name);
        }
        if(null != dateOfBirth){
            actorDoc.put("date_of_birth",dateOfBirth);
        }
        if(null != awards){
            actorDoc.put("awards", awards);
        }
        if(0!= numMovies){
            actorDoc.put("num_movies",numMovies);
        }
        documentCodec.encode(writer, actorDoc, encoderContext);
    }

    @Override
    public Class<ActorWithStringId> getEncoderClass() {
        return ActorWithStringId.class;
    }
}
