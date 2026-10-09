# Kafka Streaming — Elaborazione Ordini POC (versione 2)

> **Proof of Concept (POC)** — Questo progetto è una dimostrazione concettuale di un sistema di elaborazione di ordini basato su Kafka Streams e Spring Boot 4. Non è destinato a utilizzo in produzione.

---

## Descrizione del Progetto

Il progetto implementa una pipeline di elaborazione di ordini in tempo reale utilizzando **Apache Kafka Streams**. L'obiettivo è simulare un flusso completo di gestione degli ordini, dalla generazione dell'evento alla notifica finale, passando attraverso una fase di elaborazione con gestione degli errori, retry e invio nella Dead Letter Queue (DLQ).

Il flusso di elaborazione segue questi passaggi:

1. **Generazione eventi**: un producer genera ordini con un tasso di errore simulato configurabile. Il carico di lavoro è controllato da parametri (`load-generator.duration`, `load-generator.workers`).
2. **Elaborazione in streaming**: Kafka Streams consuma gli ordini da `orders-topic`, li elabora tramite `EnterpriseOrderProcessor` e produce un risultato di routing (`OrderRoutingResult`) con stato `SUCCESS`, `RETRY` o `DLQ`.
3. **Routing per ramo**: il flusso viene ramificato con filtri DSL:
   - Gli ordini **elaborati con successo** vengono inviati al topic `notifications-topic`.
   - Gli ordini **falliti (retry)** vengono inviati al topic `orders-retry`.
   - Gli ordini **falliti definitivamente** vengono inviati al topic `orders-dlq`.
4. **Retry con contatore persistente**: il numero di tentativi è tracciato in uno **state store RocksDB** (`order-retry-counts`) con changelog, gestito internamente alla topologia. Dopo un massimo di 3 tentativi, l'ordine è indirizzato alla DLQ.
5. **Dead Letter Queue**: gli ordini che superano i retry vengono inviati alla DLQ (`orders-dlq`) direttamente dalla topologia, con header diagnostici e payload compattato. Finiscono in DLQ anche gli ordini senza `orderId` (non elaborabili: un retry non può risolverli); i record con valore nullo (tombstone) vengono ignorati.
6. **Producer error handling**: i fallimenti del producer vengono intercettati da `KafkaProducerListenerConfig` e reindirizzati alla DLQ tramite `DlqPublisher`. Il record in DLQ ha lo stesso schema di quello scritto dalla topologia, perché entrambi lo costruiscono con `DlqService`.

---

## Caratteristiche

### Topic Kafka utilizzati

| Topic | Descrizione |
|---|---|
| `orders-topic` | Topic sorgente contenente gli ordini generati |
| `orders-retry` | Topic di retry per ordini che hanno fallito l'elaborazione iniziale |
| `notifications-topic` | Topic di destinazione per ordini elaborati con successo |
| `orders-dlq` | Dead Letter Queue per ordini che non possono essere elaborati dopo i retry |

### Elaborazione in Streaming

- La topologia Kafka Streams è definita in `OrderStreamTopology`.
- Utilizza un **processor custom** (`EnterpriseOrderProcessor`) estendendo `ContextualProcessor<String, Order, String, OrderRoutingResult>`.
- Unisce (`merge`) i flussi di `orders-topic` e `orders-retry` in un unico stream per l'elaborazione.
- Utilizza `filter` e `mapValues` per ramificare l'output in base a `OrderRoutingResult.RoutingStatus` (`SUCCESS`, `RETRY`, `DLQ`).
- Utilizza uno **state store** (`order-retry-counts`, tipo `KeyValueStore<String, Integer>`) per tracciare i tentativi di retry per ciascun ordine in modo persistente.
- Gli ordini sono modellati come **Java Records** (`Order`).
- Garanzia di processamento: **exactly-once** (`exactly_once_v2`).

### Strategia di Retry

- **State store persistente**: il contatore dei tentativi è memorizzato in RocksDB con changelog, non più in-memory.
- **`EnterpriseOrderProcessor`**: gestisce la logica di retry con `MAX_RETRIES = 3`. Dopo il superamento del limite, l'ordine viene inviato alla DLQ e il contatore viene cancellato.
- **`OrderRoutingResult`**: record che incapsula l'ordine e lo stato di routing, usato per il forwarding all'interno del processor.

### Gestione Degli Errori e DLQ

- **`DlqService`**: unico punto in cui si costruisce un record per la DLQ (chiave = `orderId`, valore = `Order` compattato, header diagnostici). Lo usano sia la topologia sia `DlqPublisher`, quindi la DLQ ha un solo schema. Ogni campo è limitato in dimensione, altrimenti un messaggio scartato perché troppo grande verrebbe rifiutato anche dalla DLQ: `customerId` e `productId` sono troncati a 64 caratteri (`StringUtils.abbreviate`) e `dlq-exception-message` a 200. Header: `workerId` (`unknown` se assente), `dlq-exception-class`, `dlq-exception-message`. Il timestamp del record Kafka non è mai negativo (il valore originale resta nel payload).
- **`DlqPublisher`**: pubblica sulla DLQ tramite `KafkaTemplate` i record costruiti da `DlqService`. Lo usa `KafkaProducerListenerConfig` per i fallimenti di invio sul topic degli ordini; la topologia scrive sulla DLQ direttamente con `.to(...)`.
- **`KafkaProducerListenerConfig`**: ascolta gli errori del producer su `KafkaTemplate` e redirige gli ordini falliti alla DLQ. Filtra gli errori non relativi al topic `orders-topic` per evitare invii ricorsivi.
- **Simulazione errori producer**: `OrderProducer` genera occasionalmente un payload di 10000 caratteri per simulare errori di dimensione del messaggio, configurabili tramite `load-generator.error-rate`. Simula inoltre errori non recuperabili tramite i parametro `load-generator.error-timestamp-modulo` e `load-generator.error-timestamp-threshold`

### Configurazione

- **Bootstrap servers**: configurabile tramite variabile d'ambiente `KAFKA_BOOTSTRAP_SERVERS`.
- **Tasso di errore simulato**: configurabile in `application.yml` (`load-generator.error-rate`, `error-timestamp-modulo`, `error-timestamp-threshold`).
- **Numero di partizioni**: configurabile (`app.topics.partitions`).
- **Pulisce i topic all'avvio**: `app.topics.clean-on-startup` (default `true`) cancella i topic esistenti all'avvio tramite `KafkaTopicCleaner`.
- **Parametri producer/consumer**: serializzatori, compressione, idempotenza, acks, batch-size, linger.ms.

---

## Tecnologie Utilizzate

| Tecnologia | Versione / Dettaglio |
|---|---|
| **Java** | 21 |
| **Spring Boot** | 4.1.1 |
| **Apache Kafka** | 7.9.9 (broker), 4.x (Streams) |
| **Spring for Apache Kafka** | spring-boot-starter-kafka |
| **Lombok** | Riduzione del boilerplate (`@Slf4j`) |
| **Jackson** | Serializzazione/deserializzazione JSON (`JacksonJsonSerde`, `JsonSerializer`/`JsonDeserializer`) |
| **Apache Commons Lang3** | Utilità (`StringUtils.abbreviate`) |
| **Maven** | Build tool |

### Dipendenze principali

- `spring-boot-starter-kafka` — integrazione Spring con Kafka.
- `kafka-streams` — elaborazione in streaming stateful e stateless.
- `spring-kafka-test` — utilities per testing (scope `test`).
- `kafka-streams-test-utils` — `TopologyTestDriver` per test della topologia (scope `test`).
- `lombok` — annotazioni per logging.
- `commons-lang3` — utility per stringhe e eccezioni.

---

## Struttura del Progetto

```
src/main/java/com/example/kafka/
├── KafkaStreamingApplication.java          # Entry point Spring Boot (@EnableKafkaStreams)
├── config/
│   ├── AppKafkaProperties.java             # Configurazione centralizzata dei topic (properties)
│   ├── KafkaTopicConfig.java               # Provisioning automatico dei topic (NewTopic beans)
│   ├── KafkaTopicCleaner.java             # Cancellazione topic all'avvio (clean-on-startup)
│   ├── KafkaSerdeConfig.java               # Serde custom per Order (JacksonJsonSerde)
│   ├── KafkaProducerListenerConfig.java    # Listener errori producer → DLQ
│   └── SchedulingConfig.java               # Configurazione ThreadPoolTaskScheduler
├── model/
│   └── Order.java                         # Modello ordine (record)
├── producer/
│   ├── OrderEventGenerator.java           # Generatore di carico con workers e metriche
│   ├── OrderGeneratorStartup.java         # Avvio automatico al ready dell'app
│   └── OrderProducer.java                 # Producer Kafka per gli ordini
├── stream/
│   ├── OrderProcessor.java                # Logica di elaborazione con simulazione errori
│   ├── EnterpriseOrderProcessor.java      # Processor Kafka Streams custom (retry + DLQ)
│   ├── OrderRoutingResult.java            # Record di routing (order + stato)
│   └── OrderStreamTopology.java           # Definizione della topologia Streams
├── dlq/
│   ├── DlqService.java                    # Costruzione dei record DLQ (schema unico, campi limitati)
│   └── DlqPublisher.java                  # Invio sulla DLQ via KafkaTemplate (errori del producer)
├── inspector/
│   └── KafkaTopicInspector.java           # Utility standalone per ispezionare topic (read_committed)
└── util/
    └── JsonUtils.java                      # Utility JSON per i log
```

### Test

```
src/test/java/com/example/kafka/
├── config/KafkaProducerListenerConfigTest.java   # Errori del producer → DLQ
├── dlq/DlqServiceTest.java                       # Costruzione dei record DLQ
├── dlq/DlqPublisherTest.java                     # Invio sulla DLQ
└── stream/OrderStreamTopologyTest.java           # Test della topologia con TopologyTestDriver
```

---

## Configurazione

### Prerequisiti

- **Java 21**
- **Docker** (per il broker Kafka tramite docker-compose)
- **Maven 3.8+**

### Avvio Rapido

```bash
# Avvia il broker Kafka e l'interfaccia grafica (Kafka UI)
docker compose up -d

# Avvia l'applicazione Spring Boot
mvn spring-boot:run
```

Una volta avviato, l'interfaccia di management di Kafka è disponibile all'URL `http://localhost:18080`.

### Variabili d'Ambiente

```bash
# Per cambiare il bootstrap server
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
```

### Parametri configurabili in `application.yml`

```yaml
# Generatore di carico
load-generator:
  duration: 1s
  workers: 4
  error-rate: 0.00001
  error-timestamp-modulo: 1000
  error-timestamp-threshold: 2

# Topic
app:
  topics:
    orders: orders-topic
    notifications: notifications-topic
    partitions: 5
    clean-on-startup: true
  retry-topics:
    orders: orders-retry
  dlq-topics:
    orders: orders-dlq
  state-store: order-retry-counts

# Kafka Streams
spring:
  kafka:
    streams:
      application-id: orders-stream-processing-group
      properties:
        processing.guarantee: exactly_once_v2
        auto.offset.reset: earliest
        num.stream.threads: 5
```

---

## Avvio e Verifica

```bash
# Compila il progetto
mvn clean package

# Avvia l'applicazione
mvn spring-boot:run

# Verifica i topic creati (con kafka-topics)
kafka-topics --bootstrap-server localhost:9092 --list

# Esegui i test
mvn test
```

I log dell'applicazione mostreranno:

- **Generazione di ordini**: il carico di lavoro viene generato con `load-generator.duration` di durata e `load-generator.workers` worker paralleli. Al termine, viene loggato il throughput (messaggi al secondo) e le statistiche di invio.
- **Elaborazione in streaming**: gli ordini vengono elaborati con ramificazione verso `notifications-topic`, `orders-retry` o `orders-dlq`.
- **Retry e DLQ**: gli ordini falliti vengono retrycati fino a 3 volte, quindi inviati alla DLQ con header diagnostici.

### Ispezione dei topic

Il progetto include `KafkaTopicInspector`, un utility standalone che consuma un topic con `isolation.level=read_committed` e restituisce un report con gli offset e il conteggio dei record effettivi:

```bash
# Ispeziona il topic notifications-topic
mvn exec:java -Dexec.mainClass="com.example.kafka.inspector.KafkaTopicInspector"
```

---

## Test

La topologia Kafka Streams è testata con `TopologyTestDriver` (`OrderStreamTopologyTest`); gli altri componenti con test unitari.

| Test | Descrizione |
|---|---|
| `successfulOrderGoesToNotifications` | Un ordine elaborato correttamente va su `notifications-topic` e non lascia stato |
| `failedOrderGoesToRetryWithFailureMetadata` | Un ordine che fallisce viene inviato 3 volte a `orders-retry` |
| `orderFailingEveryTimeIsRetriedThreeTimesThenSentToDlq...` | Dopo 3 retry l'ordine va in `orders-dlq` con chiave `orderId` e header diagnostici; lo state store viene svuotato |
| `orderFailingOnceThenSucceedingClearsTheRetryState` | Un errore transitorio porta a una notifica, nessun messaggio in DLQ e contatore azzerato |
| `recordWithNullValueIsIgnored` | Un tombstone non produce output e non ferma lo stream thread |
| `orderWithoutOrderIdGoesStraightToDlq` | Un ordine senza `orderId` va direttamente in DLQ |
| `DlqServiceTest` / `DlqPublisherTest` | Chiave, compattazione, troncamento dei messaggi, `workerId`, timestamp negativi |
| `KafkaProducerListenerConfigTest` | Gli errori sul topic ordini vanno in DLQ; gli altri topic e la DLQ stessa no (niente ricorsione) |

```bash
mvn test
```

---

## Obiettivi del POC

1. **Dimostrare l'uso di Kafka Streams** per elaborare flussi di eventi in tempo reale con ramificazione (branching) basata sul risultato.
2. **Implementare un retry stateful** utilizzando uno state store RocksDB con changelog per tracciare i tentativi, con invio definitivo alla DLQ.
3. **Gestire gli errori del producer** tramite un listener su `KafkaTemplate` che redirige i messaggi falliti alla DLQ.
4. **Compattare i payload della DLQ** per garantire che anche messaggi con dimensioni elevate vengano accettati.
5. **Esplorare l'integrazione Spring Boot 4 + Kafka 4.x** con le nuove API, configurazioni e proprietà di durata (`linger.ms` come stringa di durata).
6. **Simulare errori reali** con un tasso configurabile per osservare il comportamento della pipeline in scenari di errore.
7. **Validare il routing automatico** verso topic dedicati (retry e DLQ) in base al risultato dell'elaborazione.

---

## Limitazioni del POC

- La simulazione degli errori è basata su un tasso Casuale e sulla divisibilità del timestamp, non rappresenta carichi reali.
- Non è presente un meccanismo di deduplicazione o idempotenza consumer-side.
- La generazione di ordini è casuale e non rappresenta un carico reale.
- Non sono implementati controlli di validazione complessi sugli ordini.
- Il topic DLQ è definito ma non consumato attivamente in questo POC.

---

*Questo progetto è un Proof of Concept (POC) realizzato a scopo dimostrativo e formativo.*
