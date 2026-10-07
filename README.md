# Kafka Streaming - Order Processing POC

> **Proof of Concept (POC)** — Questo progetto è una dimostrazione concettuale di un sistema di elaborazione di ordini basato su Kafka Streams e Spring Boot 4. Non è destinato a utilizzo in produzione.

---

## Descrizione del Progetto

Il progetto implementa una pipeline di elaborazione di ordini in tempo reale utilizzando **Apache Kafka Streams**. L'obiettivo è simulare un flusso completo di gestione degli ordini, dalla generazione dell'evento alla notifica finale, passando attraverso una fase di elaborazione con gestione degli errori e retry automatici.

Il flusso di elaborazione segue questi passaggi:

1. **Generazione eventi**: un producer genera ordini con un tasso di errore simulato configurabile.
2. **Elaborazione in streaming**: Kafka Streams consuma gli ordini, li processa e produce un risultato di successo o fallimento.
3. **Routing per ramo**:
   - Gli ordini **elaborati con successo** vengono inviati al topic `notifications`.
   - Gli ordini **falliti** vengono inviati al topic `orders-retry`.
4. **Retry asincrono**: un listener consumer processa gli ordini in coda di retry con politica di retry esponenziale (backoff).
5. **Dead Letter Queue**: se tutti i retry sono esauriti, l'ordine viene inviato alla DLQ (`orders-dlq`) per analisi successiva.

---

## Caratteristiche

### Topic Kafka utilizzati

| Topic | Descrizione |
|---|---|
| `orders-topic` | Topic sorgente contenente gli ordini generati |
| `orders-retry` | Topic di retry per ordini che hanno fallito l'elaborazione iniziale |
| `notifications-topic` | Topic di destinazione per ordini elaborati con successo |
| `orders-dlq` | Dead Letter Queue per ordini che non possono essere elaborati dopo tutti i retry |

### Elaborazione in Streaming

- La topologia Kafka Streams è definita in `OrderStreamTopology`.
- Utilizza `KStream.process()` per applicare una logica custom di processing.
- Utilizza `KStream.split()` per ramificare il flusso in base al risultato (`success` / `failure`).
- Gli ordini sono modellati come **Java Records**.

### Strategia di Retry

- **Kafka Streams**: elaborazione *exactly-once* (`exactly_once_v2`) tra i topic Kafka; il listener di retry legge con `isolation.level=read_committed`.
- **Un solo schema per topic**: `notifications-topic` contiene sempre `Order`, `orders-dlq` sempre `OrderRetry` (con header `dlq-source`, `dlq-exception-class`, `dlq-exception-message`).
- **Listener di Retry**: utilizza l'annotazione `@Retryable` di Spring con backoff esponenziale.
- Contatore dei tentativi: `OrderRetryAttemptCounter` serve solo per etichettare i log (i tentativi sono decisi da `@Retryable`).
- Pubblicazione in DLQ centralizzata in `DlqPublisher`: il payload viene compattato (campi troncati) perché un messaggio scartato per dimensione non venga rifiutato anche dalla DLQ.
- Error Handler personalizzato: `OrderRetryKafkaListenerErrorHandler` gestisce gli errori del listener Kafka.

### Configurabilità

- **Bootstrap servers**: configurabile tramite variabile d'ambiente `KAFKA_BOOTSTRAP_SERVERS`.
- **Tasso di errore simulato**: configurabile in `application.yml` (`load-generator.error-rate`).
- **Numero di partizioni**: configurabile (`app.topics.partitions`).
- **Parametri producer/consumer**: serializzatori, compressione, idempotenza, acks, batch-size, linger.ms.

---

## Tecnologie Utilizzate

| Tecnologia | Versione / Dettaglio |
|---|---|
| **Java** | 21 |
| **Spring Boot** | 4.1.1 |
| **Apache Kafka** | 4.x (Streams) |
| **Spring for Apache Kafka** | spring-kafka-starter |
| **Lombok** | Riduzione del boilerplate (`@Slf4j`, record) |
| **Jackson** | Serializzazione/deserializzazione JSON |
| **Maven** | Build tool |

### Dipendenze principali

- `spring-boot-starter-kafka` — integrazione Spring con Kafka.
- `kafka-streams` — elaborazione in streaming stateful e stateless.
- `spring-kafka-test` — utilities per testing (scope `test`).
- `lombok` — annotazioni per logging e code generation.

---

## Struttura del Progetto

```
src/main/java/com/example/kafka/
├── config/
│   ├── AppKafkaProperties.java          # Configurazione centralizzata dei topic
│   ├── KafkaProducerListenerConfig.java # Configurazione producer e listener
│   ├── KafkaSerdeConfig.java            # Serde custom per Order e OrderRetry
│   ├── KafkaTopicCleaner.java           # Utility per cancellare i topic all'avvio
│   ├── KafkaTopicConfig.java            # Provisioning automatico dei topic
│   └── SchedulingConfig.java            # Configurazione task scheduling
├── model/
│   ├── Order.java                       # Modello ordine (record)
│   └── OrderRetry.java                  # Modello ordine per retry (record)
├── producer/
│   ├── OrderEventGenerator.java         # Generatore casuale di ordini
│   ├── OrderGeneratorStartup.java       # Avvio automatico del generatore
│   └── OrderProducer.java               # Producer Kafka per gli ordini
├── stream/
│   ├── OrderProcessingResult.java       # Risultato elaborazione (success/failure)
│   ├── OrderProcessor.java              # Logica di elaborazione ordine
│   ├── OrderProcessingProcessor.java    # Processor Kafka Streams custom
│   └── OrderStreamTopology.java         # Definizione della topologia Streams
├── dlq/
│   └── DlqPublisher.java                # Pubblicazione unificata sulla DLQ
├── listener/
│   ├── OrderRetryListener.java          # Listener per il topic di retry
│   ├── OrderRetryAttemptCounter.java    # Contatore tentativi per ordine
│   └── OrderRetryKafkaListenerErrorHandler.java # Error handler retry
└── util/
    └── JsonUtils.java                    # Utility JSON per i log
```

---

## Configurazione

### Prerequisiti

- **Java 21**
- **Apache Kafka** in esecuzione (default: `localhost:9092`)
- **Maven 3.8+**

### Avvio Rapido

```bash
# Avvia il broker Kafka (es. con Docker)
docker compose up -d

# Avvia l'applicazione Spring Boot
mvn spring-boot:run
```

Una volta avviato docker compose è disponibile anche la UI di management di Kafka all'url ```http://localhost:18080```

### Variabili d'Ambiente

```bash
# Per cambiare il bootstrap server
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
```

### Parametri configurabili in `application.yml`

```yaml
# Tasso di errore simulato (0.0 = nessun errore, 1.0 = sempre errore)
load-generator:
  error-rate: 0.00001
  retry-error-rate: 0.5   # errori nel listener di retry (percorso retry -> DLQ)

# Topic
app:
  topics:
    orders: orders-topic
    notifications: notifications-topic
    partitions: 5
  retry-topics:
    orders: orders-retry
  dlq-topics:
    orders: orders-dlq

# Kafka Streams
  streams:
    application-id: orders-stream-processing-group
    properties:
      processing.guarantee: exactly_once_v2
      auto.offset.reset: earliest
      num.stream.threads: 5
```

---

## Obiettivi del POC

1. **Dimostrare l'uso di Kafka Streams** per elaborare flussi di eventi in tempo reale con ramificazione (branching) basata sul risultato.
2. **Valutare una strategia di retry ibrida**: retry gestito dalla topologia Streams per il fallimento iniziale, e retry gestito da un listener asincrono per il fallimento del retry stesso.
3. **Esplorare l'integrazione Spring Boot 4 + Kafka 4.x** con le nuove API e configurazioni.
4. **Simulare errori reali** con un tasso configurabile per osservare il comportamento della pipeline in scenari di errore.
5. **Validare il routing automatico** verso topic dedicati (retry e DLQ) in base al risultato dell'elaborazione.

---

## Limitazioni del POC

- Il contatore dei tentativi di retry (`OrderRetryAttemptCounter`) è in-memory e non persiste su disco o storage distribuito.
- Non è presente un meccanismo di deduplicazione o idempotenza consumer-side.
- La generazione di ordini è casuale e non rappresenta un carico reale.
- Non sono implementati controlli di validazione complessi sugli ordini.
- Il DLQ topic è definito ma non consumato attivamente.

---

## Avvio e Verifica

```bash
# Compila il progetto
mvn clean package

# Avvia l'applicazione
mvn spring-boot:run

# Verifica i topic creati (con kafka-topics.sh)
kafka-topics.sh --list --bootstrap-server localhost:9092
```

I log dell'applicazione mostreranno:
- Generazione di ordini casuali in continuo per `load-generator.duration` con `load-generator.workers` worker; a fine test viene loggato il throughput.
- Elaborazione in streaming con ramificazione.
- Retry automatico per ordini falliti.
- Notifiche per ordini elaborati con successo.

---

*Questo progetto è un Proof of Concept (POC) realizzato a scopo dimostrativo e formativo.*
