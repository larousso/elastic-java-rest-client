package elastic;

import io.vavr.collection.List;
import io.vavr.control.Option;
import org.apache.http.HttpHost;

/**
 * Created by 97306p on 07/06/2017.
 */
public class Settings {

    public final List<HttpHost> hosts;
    public final Option<String> username;
    public final io.vavr.control.Option<String> password;
    public final Integer connectionTimeout;
    public final Integer socketTimeout;

    Settings(List<HttpHost> hosts, Option<String> username, Option<String> password, Integer connectionTimeout, Integer socketTimeout) {
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }

    public static Settings create(List<HttpHost> hosts) {
        return new Settings(hosts, Option.none(), Option.none(), 1000, 30000);
    }

    public Settings withUsername(String username) {
        return new Settings(hosts, Option.of(username), password, connectionTimeout, socketTimeout);
    }

    public Settings withUsername(Option<String> username) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout);
    }

    public Settings withPassword(String password) {
        return new Settings(hosts, username, Option.of(password), connectionTimeout, socketTimeout);
    }

    public Settings withPassword(Option<String> password) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout);
    }

    public Settings withConnectionTimeout(Integer connectionTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout);
    }
    public Settings withSocketTimeout(Integer socketTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout);
    }
    public Settings withMaxRetryTimeout(Integer maxRetryTimeout) {
        return new Settings(hosts, username, password, connectionTimeout, socketTimeout);
    }
}
