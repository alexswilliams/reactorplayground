package io.github.alexswilliams.testproject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("UnnecessaryLocalVariable")
public final class Main {
    
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    private static final Map<String, MonoSink<String>> trackedMessaged = new ConcurrentHashMap<>();
    
    
    public static void main(final String[] args) throws InterruptedException {
        
        final Scheduler scheduler = Schedulers.newParallel("metrics-publisher", 4, true);
        
        
        // We see the request come through in one direction.
        final String messageId = "Some-message-id";
        final String messageType = "outgoing.messageType";
        
        // Register in a map of message ID to completion callback:
        Mono.<String>create(monoSink -> trackedMessaged.computeIfAbsent(messageId, ignored -> monoSink))
                .subscribeOn(Schedulers.immediate()) // Adding the sink to the map needs to happen within the main thread for this demo.
                .publishOn(scheduler) // Everything else can be asynchronous.
                .log()
                // Add the elapsed time between `subscribe` being called and this point as a tuple.
                .elapsed()
                .timeout(Duration.ofSeconds(2), scheduler)
                .subscribe(
                        // on success
                        timing -> logger.info("Observed message: {}/{} -> {} (in {}ms)",
                                messageId, messageType, timing.getT2(), timing.getT1()),
                        // on error (likely just timeouts?)
                        throwable -> logger.error("Error occurred: {}", throwable.getMessage()),
                        // on completion
                        () -> trackedMessaged.remove(messageId)
                );
        
        
        // Change to > 2_000 to experience a timeout.
        // e.g. would log: Error occurred: Did not observe any item or terminal signal within 2000ms in 'elapsed' (and no fallback has been configured)
        Thread.sleep(1_000);
        
        
        // A while later we see the response come through in the opposite direction.
        final String replyId = messageId;
        final String replyType = "reply.messageType";
        
        // See if there's any incoming message to acknowledge, and if so, acknowledge it.
        final MonoSink<String> messageAcker = trackedMessaged.get(replyId);
        if (messageAcker == null) {
            logger.warn("Could not find request message for response {}", replyId);
        } else {
            messageAcker.success(replyType);
            // will log   Observed message: Some-message-id/outgoing.messageType -> reply.messageType (in 1016ms)
        }
        
        
        // Sleep a bit here to satisfy ourselves we've not blocked the main thread at all.
        Thread.sleep(1_000);
        logger.info("Exiting");
    }
}
