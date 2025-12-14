# Booking Ingestion Service

Bookings Ingestion Service written in **Kotlin** using **Spring Boot**, **Kafka (Confluent Protobuf)**, **R2DBC MySQL**, and **jOOQ**.

The service consumes *Books* events from Kafka, persists them to a MySQL database, and exposes a REST API to query bookings by position ID.

---

## Features

- Consume `Books` protobuf messages from Kafka (Confluent Schema Registry).
- Persist data to MySQL in a reactive / non-blocking fashion using **R2DBC**.
- Database schema managed with **Flyway**.
- Type-safe SQL using **jOOQ** code generation.
- REST endpoint to query bookings by position.

---

## Tech Stack

- **Language**: Kotlin (JDK 21)
- **Framework**: Spring Boot 3.5.x
- **Messaging**: Apache Kafka, Confluent Protobuf serializer/deserializer
- **Database**: MySQL
- **DB Access**:
  - R2DBC MySQL driver
  - jOOQ 3.19 (reactive usage)
  - Flyway for migrations
- **Build**: Maven

---


## Database & Migrations

- Database: `BOOKING_INGESTION_DB`
- Flyway migration scripts live under `src/main/resources/db/migration`.
- jOOQ DDL used for code generation is under `src/main/resources/db/generate`.

### Running Migrations

Migrations run automatically on application startup (Flyway enabled).  
You can also trigger them manually:

```bash
mvn -Dflyway.configFiles=src/main/resources/flyway.conf flyway:migrate
```

(Or just rely on Spring Boot’s Flyway auto-configuration.)

---

## jOOQ Code Generation

Configured via `jooq-codegen-maven` plugin:

- Uses `DDLDatabase` to read DDL scripts and generate Kotlin classes.
- Output directory: `target/generated-sources/jooq`.
- Package: `ch.innuvation.bookingingestion.jooq`.

Run generation:

```bash
mvn generate-sources
```

---

## Running Locally

### Prerequisites

- JDK 21
- Maven
- MySQL running locally on `localhost:3306` with:
  - DB: `BOOKING_INGESTION_DB`
  - User: `books`
  - Password: `books`
- Kafka broker on `localhost:9092` with:
  - SASL/SCRAM credentials (`admin` / `password` for the consumer, `client` for test producer).
- Confluent Schema Registry on `http://localhost:8081`  
  (or `http://localhost:9010/apis/ccompat/v7` for the dev profile, depending on your setup).

### Steps

1. **Build**

   ```bash
   mvn clean install
   ```

2. **Run the service**

   ```bash
   mvn spring-boot:run -Dspring-boot.run.profiles=dev
   ```

   The app starts on `http://localhost:8097`.

3. **Produce test `Books` messages**

   There is a small Kotlin producer that: `TestDataCreator`

   - Reads JSON test files from `src/main/resources/books`
   - Converts them to protobuf `Books` using `JsonFormat`
   - Sends them to Kafka using Confluent `KafkaProtobufSerializer`

   Make sure the topic and subject in Schema Registry match your configuration.

---

## REST API

### Get bookings by position

**Request**

```bash
curl "http://localhost:8097/bookings/position/274770"
```

- **Method**: `GET`
- **Path**: `/bookings/position/{posId}`
- **Path variable**: `posId` – position ID as `Long`

**Response**

- `200 OK` – JSON array of `BookingDto`
- `500` – in case of DB / query errors

Example response (shape):

```json
[
  {
    "evtId": 1000000004741,
    "buId": 140,
    "evtStatusId": 35,
    "veriDate": "2017-12-31",
    "bookDate": "2017-12-31",
    "valDate": "2017-12-31",
    "trxDate": "2017-12-31",
    "perfDate": "2017-12-31",
    "posId": 274770,
    "bookKindId": 123,
    "qty": 1.0,
    "extlBookText": "Some booking text"
  }
]
```

(The exact fields depend on your `BookingDto`.)

---

## Troubleshooting

### 1. `Unknown magic byte!` / deserialization issues

Cause: the consumer expects Confluent Protobuf wire format but the producer sends raw bytes.

Fix:

- Use `KafkaProtobufSerializer` on the producer.
- Ensure `schema.registry.url` is set and reachable.
- Ensure you send the right protobuf type.

### 2. `Connection refused` to Schema Registry

Cause: Schema Registry is not running at the configured URL.

Fix:

- Start Schema Registry container / service.
- Verify `schema.registry.url` in both producer and consumer.

### 3. `DetachedException` from jOOQ with R2DBC

Cause: Calling blocking `fetch()` / `execute()` with only R2DBC configured.

Fix:

- Use reactive fetching with `Publisher` → Reactor → Kotlin coroutines, e.g.:

  ```kotlin
  suspend fun getBookingsByPosId(posId: Long): List<BookingDto> =
      Flux.from(
          dsl.select(...)  // jOOQ query
      ).map { r -> /* map to BookingDto */ }
       .collectList()
       .awaitSingle()
  ```

- Mark controller and service methods as `suspend` where appropriate.

---

## Useful Commands

- Build:

  ```bash
  mvn clean install
  ```

- Run with dev profile:

  ```bash
  mvn spring-boot:run -Dspring-boot.run.profiles=dev
  ```

- Hit API:

  ```bash
  curl "http://localhost:8097/bookings/position/274770"
  ```

---

## TODO / Ideas

- Add integration tests with Testcontainers (MySQL + Kafka + Schema Registry).
- Add proper error / DLQ handling for Kafka consumer.
- Add OpenAPI / Swagger documentation.
- Add health checks for DB & Kafka readiness.
