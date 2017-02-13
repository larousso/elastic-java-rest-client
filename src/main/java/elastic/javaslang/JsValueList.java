package elastic.javaslang;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;

import java.io.IOException;


public class JsValueList extends JsonDeserializer<List<JsValue>> {
    @Override
    public List<JsValue> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode treeNode = p.getCodec().readTree(p);
        return List.ofAll(treeNode).map(Json::fromJsonNode);
    }
}
