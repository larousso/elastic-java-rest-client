package elastic.response;

import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Format;
import org.reactivecouchbase.json.mapping.Reader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import javaslang.control.Option;

/**
 * Created by adelegue on 20/10/2016.
 */
public class GetResponse {
    public final static Format<GetResponse> format = Json.format(GetResponse.class);

    public String _index;

    public String _type;

    public String _id;

    public Integer _version;

    public Boolean found;

    public JsonNode _source;

    private JsValue source() {
        if(_source != null)
            return Json.fromJsonNode(_source);
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

    public String stringify() {
        return Json.stringify(Json.toJson(this, format));
    }

    @Override
    public String toString() {
        return stringify();
    }
}
