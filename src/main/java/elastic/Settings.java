package elastic;

import javaslang.collection.List;
import javaslang.control.Option;
import org.apache.http.HttpHost;

/**
 * Created by 97306p on 07/06/2017.
 */
public class Settings {

    public final List<HttpHost> hosts;
    public final Option<String> username;
    public final javaslang.control.Option<String> password;
    public final Integer connectionTimeout;
    public final Integer socketTimeout;
    public final Integer maxRetryTimeout;

    Settings(List<HttpHost> hosts, Option<String> username, Option<String> password, Integer connectionTimeout, Integer socketTimeout, Integer maxRetryTimeout) {
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.maxRetryTimeout = maxRetryTimeout;
    }

    public static Settings create(List<HttpHost> hosts) {
        return new Settings(hosts, Option.none(), Option.none(), 1000, 30000, 30000);
    }

    public Settings withUsername(String username) {
        return new Settings(hosts, Option.of(username), password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }

    public Settings withUsername(Option<String> username) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }

    public Settings withPassword(String password) {
        return new Settings(hosts, username, Option.of(password), connectionTimeout, socketTimeout, maxRetryTimeout);
    }

    public Settings withPassword(Option<String> password) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }

    public Settings withConnectionTimeout(Integer connectionTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }
    public Settings withSocketTimeout(Integer socketTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }
    public Settings withMaxRetryTimeout(Integer maxRetryTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout, maxRetryTimeout);
    }
}
