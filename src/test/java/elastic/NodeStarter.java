package elastic;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;

import javax.annotation.PreDestroy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeStarter {

    private final static Logger LOGGER = LoggerFactory.getLogger(NodeStarter.class);

    private final Node esNode;

    private final String homePath;

    private final Integer httpPort;

    public NodeStarter(Integer httpPort) {
        this.httpPort = httpPort == null ? 9200 : httpPort;
        this.homePath = "target/elasticsearch-" + UUID.randomUUID().toString();
        this.esNode = startNode(homePath);
    }

    private Node startNode(String homePath) {
        LOGGER.info("Starting local es node with http.port {}", httpPort);
        Node node = nodeBuilder()
                .settings(Settings.settingsBuilder()
                        .put("path.home", homePath)
                        .put("http.port", httpPort)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .local(true)
                .node();

        node.start();

        node.client().admin().cluster()
                .prepareHealth()
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueMinutes(1))
                .execute()
                .actionGet();

        return node;
    }

    @PreDestroy
    public void closeNode() {
        LOGGER.info("Closing local es node");
        esNode.close();
        Path rootPath = Paths.get(homePath);
        try {
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .peek(System.out::println)
                    .forEach(File::delete);
        } catch (IOException e) {
            LOGGER.error("Error deleting ES datas", e);
        }
    }
}
