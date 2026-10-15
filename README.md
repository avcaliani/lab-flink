<img src="https://flink.apache.org/img/logo/png/100/flink_squirrel_100_color.png" align="right" height="64px"/>

# Lard Lad Exchange

![License](https://img.shields.io/github/license/avcaliani/deep-dive-flink?logo=apache&color=lightseagreen)
![Java](https://img.shields.io/badge/Java-17-FF7800?logo=openjdk&logoColor=white)
![Apache Flink](https://img.shields.io/badge/apache--flink-2.0.x-ff4757.svg)
![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

My repository with [Apache Flink](https://flink.apache.org) learnings.

The app name is "Lard Lad Exchange", inspired by Lard Lad Donuts, Springfield's giant-donut landmark from *The Simpsons*.

## Quick Start

#### 01 - **Download** user mocked data

```bash
mkdir -p ./data/raw/users
curl -o "data/raw/users/users.csv" \
  "https://raw.githubusercontent.com/avcaliani/kafka-in-docker/refs/heads/main/scripts/users.csv"
```

#### 02 - Start the containers 🐳

> 💡 [Flink Local Installation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/try-flink/local_installation/)

```bash
# 💡 To stop just type `docker compose down`
docker compose up -d
```

> [!TIP]
> To check the Kafka Topics and Messages or Flink Application STDOUT, check the links below.  
> - **Kafka UI** ➜ http://localhost:8080  
> - **Flink Dashboard** ➜ http://localhost:8081

#### 03 - Start **kafka producer** ([ref](https://github.com/avcaliani/kafka-in-docker/tree/main/scripts)) 👇

```bash
docker compose exec kafka-dev /opt/scripts/donu-transactions.sh
```

#### 04 - **Run** the application

```bash
# Pipeline - Dummy
#   Pretty simple pipeline, it just prints the list of customer in a CSV file. 
./run.sh --pipeline "dummy" --bucket "/data"
  
# Pipeline - Validate Transactions
#   Classify the transactions as correct/incorrect, 
#   enrich and forward them to another topic, check the diagram.
./run.sh --pipeline "validate-transactions" \
      --bucket "/data" \
      --kafka-brokers "kafka-dev:29092"
```

#### 05 - Restart from a checkpoint (optional)

Checkpoints are kept when a job is cancelled, at `data/flink/checkpoint/<job-id>/chk-<n>`.  
To resume from one, pass `-s` as the **first** argument of the script.

```bash
./run.sh -s "/data/flink/checkpoint/<job-id>/chk-<n>" \
      --pipeline "validate-transactions" \
      --bucket "/data" \
      --kafka-brokers "kafka-dev:29092"
```

> [!IMPORTANT]
> The valid transactions topic is written with Kafka transactions (`EXACTLY_ONCE`).  
> Consumers of that topic must set `isolation.level=read_committed`, otherwise they will also read records
> from aborted or still open transactions.

## Tests

```bash
./gradlew test
```

## Pipeline - Validate Transactions

```mermaid
---
config:
  theme: 'base'
  look: 'handDrawn'
  themeVariables:
    primaryColor: '#f6f8fa'
    primaryTextColor: '#24292f'
    primaryBorderColor: '#d0d7de'
    lineColor: '#0969da'
    secondaryColor: '#afb8c1'
    tertiaryColor: '#ddf4ff'
    background: '#ffffff'
    edgeLabelBackground: '#ffffff'
---
graph LR
  app_txn s01@-->|publishes<br>transactions| q_txn_v1

  q_txn_v1 s02@--> app_flink
  st_user --> app_flink

  app_flink s03@-->|valid data| st_txn
  app_flink s04@-->|invalid data| q_dead_letter

%% ----------------------------------------
%%  Style
%% ----------------------------------------

%% Applications
app_txn@{ shape: rounded, label: "Txn Service" }
app_flink@{ shape: rounded, label: "🐿️ Flink App" }

%% Queues
q_txn_v1@{ shape: das, label: "donu_txn_v1" }
q_dead_letter@{ shape: das, label: "dead_letter"}

%% Storage
st_user@{ shape: disk, label: "users.csv" }
st_txn@{ shape: das, label: "donu_exec_txn_v1" }

%% Animation
s01@{ animate: true }
s02@{ animate: true }
s03@{ animate: true }
s04@{ animate: true }

%% Arrow Colors
linkStyle 3 stroke:#00b894
linkStyle 4 stroke:#d63031
```
