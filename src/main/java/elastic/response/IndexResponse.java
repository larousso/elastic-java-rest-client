package elastic.response;

import com.fasterxml.jackson.databind.JsonNode;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

/**
 * Created by adelegue on 08/10/2016.
 */
public class IndexResponse {

    public static final Reader<IndexResponse> read = Json.reads(IndexResponse.class);

    public JsonNode _shards;

    public String _index;

    public String _type;

    public String _id;

    public Integer _version;

    public Boolean created;

    public Boolean found;

}
