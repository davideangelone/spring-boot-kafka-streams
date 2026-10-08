package com.example.kafka.stream;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class OrderStreamTopology {

    private final AppKafkaProperties properties;
    private final OrderProcessor orderProcessor;

    public OrderStreamTopology(AppKafkaProperties properties, OrderProcessor orderProcessor) {
        this.properties = properties;
        this.orderProcessor = orderProcessor;
    }

    @Bean
    public KStream<String, Order> orderStream(StreamsBuilder streamsBuilder,
                                              Serdes.StringSerde stringSerde,
                                              Serde<Order> orderSerde) {

        // 1. Definiamo lo State Store per i contatori dei tentativi (RocksDB + Changelog)
        StoreBuilder<KeyValueStore<String, Integer>> retryCountStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("order-retry-counts"),
                Serdes.String(),
                Serdes.Integer()
        );
        streamsBuilder.addStateStore(retryCountStoreBuilder);

        // 2. Leggiamo ENTRAMBI i topic di input
        KStream<String, Order> mainStream = streamsBuilder.stream(
                properties.getTopics().getOrders(),
                Consumed.with(Serdes.String(), orderSerde)
        );

        KStream<String, Order> retryStream = streamsBuilder.stream(
                properties.getRetryTopics().getOrders(),
                Consumed.with(Serdes.String(), orderSerde)
        );

        // 3. Uniamo i flussi ed eseguiamo il processamento custom.
        // NOTA: Il processor ora restituisce un OrderRoutingResult (contenente l'Order e il target)
        KStream<String, OrderRoutingResult> processedStream = mainStream.merge(retryStream)
                .process(() -> new EnterpriseOrderProcessor(orderProcessor), "order-retry-counts");

        // 4. Eseguiamo il routing dell'output usando i filtri nativi della DSL (molto più pulito di addSink)

        // Flusso Successi -> Notifications
        processedStream
                .filter((k, v) -> v.status() == OrderRoutingResult.RoutingStatus.SUCCESS)
                .mapValues(OrderRoutingResult::order)
                .to(properties.getTopics().getNotifications(), Produced.with(stringSerde, orderSerde));

        // Flusso Retry -> Topic di Retry
        processedStream
                .filter((k, v) -> v.status() == OrderRoutingResult.RoutingStatus.RETRY)
                .mapValues(OrderRoutingResult::order)
                .to(properties.getRetryTopics().getOrders(), Produced.with(stringSerde, orderSerde));

        // Flusso Errori Infiniti -> DLQ
        processedStream
                .filter((k, v) -> v.status() == OrderRoutingResult.RoutingStatus.DLQ)
                .mapValues(OrderRoutingResult::order)
                .to(properties.getDlqTopics().getOrders(), Produced.with(stringSerde, orderSerde));

        return mainStream;
    }
}
