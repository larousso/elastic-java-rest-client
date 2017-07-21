package elastic.response;

import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivecouchbase.json.JsPair;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
import org.reactivecouchbase.json.mapping.Writer;

import static org.reactivecouchbase.json.Syntax.$;

public class IndexResponse {

    public static final Reader<IndexResponse> reads = json -> {
        try {
            return JsResult.success(new IndexResponse(
                    json.field("_index").asOptString().getOrElse((String)null),
                    json.field("_type").asOptString().getOrElse((String)null),
                    json.field("_id").asOptString().getOrElse((String)null),
                    json.field("_version").asInteger(),
                    json.field("created").asBoolean(),
                    json.field("found").asOptBoolean().getOrElse((Boolean) null),
                    json.field("_shards")
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public static final Writer<IndexResponse> writes = response -> Json.obj(List.of(
            Option.of(response._index).map(n -> $("_index", n)),
            Option.of(response._type).map(n -> $("_type", n)),
            Option.of(response._id).map(n -> $("_id", n)),
            Option.of(response._version).map(n -> $("_version", n)),
            Option.of(response.created).map(n -> $("created", n)),
            Option.of(response.found).map(n -> $("found", n)),
            Option.of($("_shards", response._shards))
    ).flatMap(e -> e).toJavaArray(JsPair.class));

    public final String _index;

    public final String _type;

    public final String _id;

    public final Integer _version;

    public final Boolean created;

    public final Boolean found;

    public final JsValue _shards;

    public IndexResponse(String _index, String _type, String _id, Integer _version, Boolean created, Boolean found, JsValue _shards) {
        this._index = _index;
        this._type = _type;
        this._id = _id;
        this._version = _version;
        this.created = created;
        this.found = found;
        this._shards = _shards;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("IndexResponse{");
        sb.append("_shards=").append(_shards);
        sb.append(", _index='").append(_index).append('\'');
        sb.append(", _type='").append(_type).append('\'');
        sb.append(", _id='").append(_id).append('\'');
        sb.append(", _version=").append(_version);
        sb.append(", created=").append(created);
        sb.append(", found=").append(found);
        sb.append('}');
        return sb.toString();
    }
}
