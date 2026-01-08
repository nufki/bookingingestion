# Booking Ingestion Service

This Booking Ingestion Service is written in **Kotlin** using **Spring Boot**, **Kafka (Confluent Protobuf)**, **Oracle Database**, and **Spring JDBC (JdbcTemplate)**. The purpose of this project is to demonstrate how to sink high-volume messages performantly into Oracle.

---

## Features

- Consumes and persists **Books** protobuf messages from Kafka
- Persists messages to **Oracle** using bulk PL/SQL stored procedures for high-throughput upserts and deletes
- Database schema managed with **Flyway**
- REST endpoint to query bookings by position ID (**Note:** This is for demo purposes only. This service should NOT provide REST query services alongside high-throughput data ingestion!)

---

## Tech Stack

- **Language**: Kotlin (JDK 21)
- **Framework**: Spring Boot 3.5.x
- **Messaging**: Apache Kafka, Confluent Protobuf serializer/deserializer
- **Database**: Oracle 23ai Free Edition
- **DB Access**:
    - Spring JDBC (`JdbcTemplate`)
    - Flyway for migrations
- **Build**: Maven

---

## Database & Migrations

- Database schema: `books` user in `FREEPDB1` pluggable database
- Flyway migration scripts located in `src/main/resources/db/migration`
- Stored procedures for bulk upsert/delete defined in `V2__create_bulk_upsert_procedures.sql`

---

## Running Locally

### Prerequisites

- JDK 21
- Maven
- Oracle Database running locally (or in Docker) with:
    - PDB: `FREEPDB1`
    - Port: `1521`
    - User: `books`
    - Password: `books`
- Kafka broker on `localhost:9092` with SASL/SCRAM credentials
- Confluent Schema Registry at `http://localhost:9010/apis/ccompat/v7`

### Quick Start with Docker Compose

```bash
# Start all services (Kafka, Oracle, Schema Registry)
docker compose up -d

# Wait for Oracle to be healthy (check logs)
docker logs -f bookingingestion-oracle

# Run the application
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

---

### Steps

1. **Build**

   ```bash
   mvn clean install
   ```

2. **Run the service**

   ```bash
   mvn spring-boot:run -Dspring-boot.run.profiles=dev
   ```

3. **Produce test `Books` messages**

   Use the `TestDataCreator` Kotlin producer that:

    - Reads JSON test files from `src/main/resources/books`
    - Converts them to protobuf `Books` using `JsonFormat`
    - Sends them to Kafka using Confluent `KafkaProtobufSerializer`

   Ensure the topic and subject in Schema Registry match your configuration.

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
- `500` – Database or query errors

Example response:

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

---

## Performance Benefits

This service uses **Oracle PL/SQL bulk operations** (`FORALL` with `MERGE`) executed via stored procedures. This approach:

- Sends batches of thousands of rows in a single round-trip, dramatically reducing network latency
- Leverages Oracle's internal optimizations for bulk DML, resulting in lower CPU usage compared to row-by-row inserts/updates
- Guarantees atomicity per batch via the procedure's transaction scope
- Simplifies application code – no need for complex reactive streams or per-record JDBC calls

---

## Oracle Database Docker Image

For local development and CI, this project uses Oracle in Docker with the **gvenzl/oracle-free:23-slim-faststart** image:

- **Free edition** – no licensing concerns for development and testing
- **Slim** – minimal OS layers, resulting in a smaller image (~1GB) and faster pulls
- **Fast-start** – pre-configured to start quickly, reducing wait times in `docker compose up`
- **Multi-arch** – supports both x86_64 and ARM64, so it works seamlessly on Apple Silicon machines
- Exposes the standard Oracle listener on port 1521, matching the datasource URL in `application.yaml`

See `docker-compose.yaml` for the complete service definition.

---

## Testcontainers Support

For integration testing, this project uses **Testcontainers** to spin up an Oracle database in Docker, ensuring that the same bulk-upsert procedures and Flyway migrations are tested against a real Oracle instance.


---

## References

- [Oracle Database Docker Images by Gerald Venzl](https://github.com/gvenzl/oci-oracle-free)
- [Confluent Protobuf Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html)
- [Spring Boot Kafka](https://spring.io/projects/spring-kafka)
- [Flyway](https://flywaydb.org/)
