package elastic.response;

import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Format;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Created by adelegue on 08/10/2016.
 */
public class IndexResponse {

    public static final Format<IndexResponse> format = Json.format(IndexResponse.class);

    public JsonNode _shards;

    public String _index;

    public String _type;

    public String _id;

    public Integer _version;

    public Boolean created;

    public Boolean found;

    public String stringify() {
        return Json.stringify(Json.toJson(this, format));
    }

    @Override
    public String toString() {
        return stringify();
    }
}
