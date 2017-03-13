package elastic.response;

import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
import static org.reactivecouchbase.json.Syntax.*;

import javaslang.collection.List;
import org.reactivecouchbase.json.mapping.Writer;

public class SearchResponse {

    public static final Reader<SearchResponse> reads = json -> {
        try {
            return JsResult.success(new SearchResponse(
                    json.field("took").asOptInteger().getOrElse(() -> null),
                    json.field("timed_out").asOptBoolean().getOrElse(() -> null),
                    json.field("_shards"),
                    json.field("max_score").asOptInteger().getOrElse(() -> null),
                    json.field("hits").as(Hits.reads),
                    json.field("aggregations"),
                    json.field("acknowledged").asOptBoolean().getOrElse(() -> null),
                    json.field("status").asOptInteger().getOrElse(() -> null),
                    json.field("error")
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public static final Writer<SearchResponse> writes = response -> Json.obj(
            $("took", response.took),
            $("timed_out", response.timed_out),
            $("_shards", response._shards),
            $("max_score", response.max_score),
            $("hits", Json.toJson(response.hits, Hits.writes)),
            $("aggregations", response.aggregations),
            $("acknowledged", response.acknowledged),
            $("status", response.status),
            $("error", response.error)
    );



    public final Integer took;

    public final Boolean timed_out;

    public final JsValue _shards;

    public final Integer max_score;

    public final Hits hits;

    public final JsValue aggregations;

    public final Boolean acknowledged;

    public final Integer status;

    public final JsValue error;

    public SearchResponse(Integer took, Boolean timed_out, JsValue _shards, Integer max_score, Hits hits, JsValue aggregations, Boolean acknowledged, Integer status, JsValue error) {
        this.took = took;
        this.timed_out = timed_out;
        this._shards = _shards;
        this.max_score = max_score;
        this.hits = hits;
        this.aggregations = aggregations;
        this.acknowledged = acknowledged;
        this.status = status;
        this.error = error;
    }

    public JsValue error() {
        if(error == null) {
            return new JsNull();
        } else {
            return error;
        }
    }

    public static class Hits {

        public static final Reader<Hits> reads = json -> {
            try {
                return JsResult.success(new Hits(
                        json.field("total").asOptLong().getOrElse(() -> null),
                        json.field("max_score").asOptInteger().getOrElse(() -> null),
                        List.ofAll(json.field("hits").asArray())
                ));
            } catch (Exception e) {
                return JsResult.error(e);
            }
        };

        public static final Writer<Hits> writes = hits -> Json.obj(
                $("total", hits.total),
                $("max_score", hits.max_score),
                $("hits", Json.arr(hits.hits.toJavaArray()))
        );

        public Long total;

        public Integer max_score;

        public List<JsValue> hits = List.empty();

        public Hits(Long total, Integer max_score, List<JsValue> hits) {
            this.total = total;
            this.max_score = max_score;
            this.hits = hits;
        }

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
