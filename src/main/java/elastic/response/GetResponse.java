package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import javaslang.control.Option;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Format;
import org.reactivecouchbase.json.mapping.Reader;

/**
 * Created by adelegue on 20/10/2016.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class GetResponse {
    public final static Reader<GetResponse> reads = Json.reads(GetResponse.class);

    public String _index;

    public String _type;

    public String _id;

    public Integer _version;

    public Boolean found;

    public JsValue _source;

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
        sb.append(", _type='").append(_type).append('\'');
        sb.append(", _id='").append(_id).append('\'');
        sb.append(", _version=").append(_version);
        sb.append(", found=").append(found);
        sb.append(", _source=").append(_source);
        sb.append('}');
        return sb.toString();
    }
}
