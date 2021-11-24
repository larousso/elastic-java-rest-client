package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsPair;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
import org.reactivecouchbase.json.mapping.Writer;

import static org.reactivecouchbase.json.Syntax.$;

/**
 * Created by adelegue on 20/10/2016.
 */
public class GetResponse {
    public final static Reader<GetResponse> reads = json -> {
        try {
            return JsResult.success(new GetResponse(
                    json.field("_index").asOptString().getOrElse(() -> null),
                    json.field("_id").asOptString().getOrElse(() -> null),
                    json.field("_version").asOptInteger().getOrElse(() -> null),
                    json.field("found").asOptBoolean().getOrElse(() -> null),
                    json.field("_source")
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public static final Writer<GetResponse> writes = response -> Json.obj(List.of(
            Option.of(response._index).map(n -> $("_index", n)),
            Option.of(response._id).map(n -> $("_id", n)),
            Option.of(response._version).map(n -> $("_version", n)),
            Option.of(response.found).map(n -> $("found", n)),
            Option.of($("_source", response._source))
    ).flatMap(e -> e).toJavaArray(JsPair[]::new));

    public final String _index;

    public final String _id;

    public final Integer _version;

    public final Boolean found;

    public final JsValue _source;

    public GetResponse(String _index, String _id, Integer _version, Boolean found, JsValue _source) {
        this._index = _index;
        this._id = _id;
        this._version = _version;
        this.found = found;
        this._source = _source;
    }

    private JsValue source() {
        if(_source != null)
            return _source;
        else
            return new JsNull();
    }

    @JsonIgnore
    public <T> Option<T> as(Reader<T> reader) {
        if(found) {
            return Json.fromJson(source(), reader).asOpt();
        } else {
            return Option.none();
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("GetResponse{");
        sb.append("_index='").append(_index).append('\'');
        sb.append(", _id='").append(_id).append('\'');
        sb.append(", _version=").append(_version);
        sb.append(", found=").append(found);
        sb.append(", _source=").append(_source);
        sb.append('}');
        return sb.toString();
    }
}
