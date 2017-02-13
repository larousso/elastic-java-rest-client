package elastic.javaslang;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import elastic.response.BulkResponse.BulkItem;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import java.io.IOException;


public class BulkItemList extends JsonDeserializer<List<BulkItem>> {
    @Override
    public List<BulkItem> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode treeNode = p.getCodec().readTree(p);
        List<JsValue> values = List.ofAll(treeNode).map(Json::fromJsonNode);
        return values.map(j -> Json.fromJson(j, Json.reads(BulkItem.class)).get());
    }
}
