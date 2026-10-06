package com.example.kafka.producer;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderEventGenerator {

    private final OrderProducer producer;
    private final TaskScheduler taskScheduler;
    private final int workers;
    private final Duration duration;
    private final AtomicLong generated = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong activeWorkers = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private CompletableFuture<Void> sendsCompleted;

    private ExecutorService executor;

    public OrderEventGenerator(
            OrderProducer producer,
            TaskScheduler taskScheduler,
            @Value("${load-generator.duration:10s}") Duration duration,
            @Value("${load-generator.workers:8}") int workers) {

        this.producer = producer;
        this.taskScheduler = taskScheduler;
        this.workers = workers;
        this.duration = duration;
    }

    public void start() {

        if (!running.compareAndSet(false, true)) {
            log.warn("Load test is already running");
            return;
        }

        /*
         * Reset delle statistiche.
         */
        generated.set(0);
        sent.set(0);
        errors.set(0);
        pending.set(0);

        /*
         * Ogni nuovo worker ha il proprio CompletableFuture.
         */
        sendsCompleted = new CompletableFuture<>();

        /*
         * Importante: deve essere impostato PRIMA
         * di avviare i worker.
         */
        activeWorkers.set(workers);

        Instant endTime = Instant.now().plus(duration);

        executor = Executors.newFixedThreadPool(workers);

        log.info("Starting load test: workers={}, duration={}", workers, duration);

        for (int i = 0; i < workers; i++) {
            int workerId = i + 1;
            executor.submit(
                    () -> generate(workerId, endTime)
            );
        }

        taskScheduler.schedule(
                () -> stop(endTime),
                endTime
        );
    }

    private void stop(Instant endTime) {

        /*
         * Da questo momento i worker non devono
         * generare nuovi messaggi.
         */
        running.set(false);

        /*
         * Non accetta nuovi task.
         *
         * I worker già in esecuzione terminano
         * naturalmente perché vedono running=false.
         */
        executor.shutdown();

        /*
         * Potrebbe già essere tutto terminato.
         */
        checkCompletion();

        sendsCompleted
                .thenRun(() -> log.info("All workers and Kafka sends completed"))
                .thenRun(() -> logResults(endTime))
                .exceptionally(exception -> {
                    log.error("Error while completing load test", exception);
                    return null;
                });
    }

    private void generate(int workerId, Instant endTime) {

        while (running.get() && Instant.now().isBefore(endTime)) {

            Order order = new Order(
                    UUID.randomUUID().toString(),
                    "CUST-" + ThreadLocalRandom.current().nextInt(1, 100),
                    "PROD-" + ThreadLocalRandom.current().nextInt(1, 50),
                    ThreadLocalRandom.current().nextInt(1, 10),
                    System.currentTimeMillis()
            );

            generated.incrementAndGet();
            pending.incrementAndGet();

            try {
                producer.send(order, workerId)
                        .whenComplete((result, exception) -> {

                            pending.decrementAndGet();

                            if (exception != null) {
                                errors.incrementAndGet();
                            } else {
                                sent.incrementAndGet();
                            }

                            checkCompletion();
                        });

            } catch (Exception e) {
                pending.decrementAndGet();
                errors.incrementAndGet();
                log.error("Worker {} encountered an error", workerId);
            }
        }

        activeWorkers.decrementAndGet();
        checkCompletion();

        log.info("Worker {} completed", workerId);
    }

    private void checkCompletion() {

        /*
         * Il test può essere considerato completato
         * solo quando:
         *
         * 1. running == false
         * 2. tutti i worker sono terminati
         * 3. non ci sono più send Kafka in-flight
         */
        if (!running.get()
                && activeWorkers.get() == 0
                && pending.get() == 0) {

            sendsCompleted.complete(null);
        }
    }

    private void logResults(Instant endTime) {

        double seconds = Duration.between(endTime.minus(duration), endTime).toMillis() / 1000.0;
        long throughput = Math.round(sent.get() / seconds);

        log.info(
                "Load test completed: " +
                        "generated={} - " +
                        "sent={} - " +
                        "simulated errors={} - " +
                        "pending={} - " +
                        "throughput={} msg/s",

                generated.get(),
                sent.get(),
                errors.get(),
                pending.get(),
                throughput
        );
    }
}

