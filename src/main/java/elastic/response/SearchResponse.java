package elastic.response;

import com.fasterxml.jackson.databind.JsonNode;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

public class SearchResponse {

    public static final Reader<SearchResponse> read = Json.reads(SearchResponse.class);

    public Integer took;

    public Boolean timed_out;

    public JsonNode _shards;

    public Hits hits;

    public JsonNode aggregations;

    public Boolean acknowledged;

    public Integer status;

    public JsonNode error;

    public JsValue error() {
        if(error == null) {
            return new JsNull();
        } else {
            return Json.fromJsonNode(error);
        }
    }

    public static class Hits {

        public Long total;

        public Integer max_score;

        public java.util.List<JsonNode> hits;

        public <T> List<T> hitsAs(Reader<T> hitRead) {
            return List.ofAll(hits)
                    .map(Json::fromJsonNode)
                    .map(json -> json.field("_source"))
                    .map(json -> Json.fromJson(json, hitRead).get());
        }

    }
}
