package elastic.streams;

import akka.actor.ActorSystem;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by 97306p on 14/04/2017.
 */
public class FlowsTest {


    @Test
    public void mapAsync() throws ExecutionException, InterruptedException {
        List<Integer> integers = Source.range(0, 5)
                .via(Flows.mapAsync(i -> CompletableFuture.supplyAsync(() -> i)))
                .map(i -> {
                    System.out.println("i : "+i);
                    return i;
                })
                .runWith(Sink.seq(), ActorSystem.create())
                .toCompletableFuture()
                .get();

        assertThat(integers).contains(0, 1, 2, 3, 4, 5);
    }


    @Test
    public void backPressure() throws ExecutionException, InterruptedException {
        AtomicInteger i = new AtomicInteger(0);
        List<Integer> integers = Source.range(1, 4)
                .via(Flows.mapAsync(e -> CompletableFuture.supplyAsync(() -> {
                    i.incrementAndGet();
                    return e;
                })))
                .take(1)
                .filter(any -> true)
                .runWith(Sink.seq(), ActorSystem.create())
                .toCompletableFuture().get();
        assertThat(integers).contains(1);
        assertThat(i.get()).isEqualTo(1);
    }
}