package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import elastic.javaslang.JsValueList;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

@JsonIgnoreProperties(ignoreUnknown=true)
public class SearchResponse {

    public static final Reader<SearchResponse> reads = Json.reads(SearchResponse.class);

    public Integer took;

    public Boolean timed_out;

    public JsValue _shards;

    public Integer max_score;

    public Hits hits;

    public JsValue aggregations;

    public Boolean acknowledged;

    public Integer status;

    public JsValue error;

    public JsValue error() {
        if(error == null) {
            return new JsNull();
        } else {
            return error;
        }
    }

    public static class Hits {

        public Long total;

        public Integer max_score;

        @JsonDeserialize(using = JsValueList.class)
        public List<JsValue> hits = List.empty();

        public <T> List<T> hitsAs(Reader<T> hitRead) {
            return hits
                    .map(json -> json.field("_source"))
                    .map(json -> Json.fromJson(json, hitRead).get());
        }

        public Long total() {
            return total;
        }

        public Integer maxScore() {
            return max_score;
        }

        public List<JsValue> hits() {
            return List.ofAll(hits);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SearchResponse{");
        sb.append("took=").append(took);
        sb.append(", timed_out=").append(timed_out);
        sb.append(", _shards=").append(_shards);
        sb.append(", hits=").append(hits);
        sb.append(", aggregations=").append(aggregations);
        sb.append(", acknowledged=").append(acknowledged);
        sb.append(", status=").append(status);
        sb.append(", error=").append(error);
        sb.append('}');
        return sb.toString();
    }
}
