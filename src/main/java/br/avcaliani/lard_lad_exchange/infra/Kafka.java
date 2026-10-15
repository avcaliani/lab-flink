package br.avcaliani.lard_lad_exchange.infra;

import br.avcaliani.lard_lad_exchange.infra.serializers.KafkaDeserializer;
import br.avcaliani.lard_lad_exchange.infra.serializers.KafkaMessage;
import br.avcaliani.lard_lad_exchange.infra.serializers.KafkaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;

/**
 * <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/">
 * Flink + Kafka Documentation
 * </a>
 */
public class Kafka extends Infra {

    private final String brokers;

    public Kafka(StreamExecutionEnvironment env, String brokers) {
        super(env, "kafka");
        this.brokers = brokers;
    }

    /**
     * Create a Data Stream based on Kafka Source.
     *
     * @param topic   Kafka Topic.
     * @param groupId Kafka Consumer - Group ID.
     * @param clazz   POJO Class Kafka Message (also the 'T')
     * @return Data Stream.
     */
    public <T extends KafkaMessage> DataStream<T> read(String topic, String groupId, Class<T> clazz) {

        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(this.brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaDeserializer<>(clazz))
                .build();

        return this.env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName(topic));
    }

    /**
     * Create a Kafka Sink for a specific topic.
     * <br/><br/>
     * <b>Something about EXACTLY_ONCE...</b><br/>
     * Brokers have <code>transaction.max.timeout.ms</code> set to 15 minutes by default, and they reject producers
     * asking for a larger <code>transaction.timeout.ms</code>.
     * <code>KafkaSink</code> asks for 1 hour by default, so the timeout is lowered to 15 minutes here.
     * Keep it larger than the checkpoint interval plus the time the job needs to recover.
     * <br/>
     * Downstream consumers must set <code>isolation.level=read_committed</code>,
     * otherwise they will also read records from aborted or still open transactions.
     * <br/><br/>
     * More details in the
     * <a href="https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/connectors/datastream/kafka/#fault-tolerance">
     * KafkaSink fault tolerance docs</a>
     * and in the post about
     * <a href="https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/">
     * how exactly once works</a>.
     *
     * @param topic    Kafka Topic.
     * @param delivery Delivery Strategy.
     * @return Kafka Sink.
     */
    public <T extends KafkaMessage> KafkaSink<T> sink(String topic, DeliveryGuarantee delivery) {
        var sinkBuilder = KafkaSink.<T>builder()
                .setBootstrapServers(this.brokers)
                .setRecordSerializer(new KafkaSerializer<>(topic))
                .setDeliveryGuarantee(delivery);

        if (delivery == EXACTLY_ONCE) {
            sinkBuilder
                    .setProperty("transaction.timeout.ms", "900000")
                    .setTransactionalIdPrefix("lard-lad-exchange--"); // Mandatory when using Exactly Once
        }
        return sinkBuilder.build();
    }

    /**
     * Documentation 👉 {@link #sink(String, DeliveryGuarantee)}
     */
    public <T extends KafkaMessage> KafkaSink<T> sink(String topic) {
        return sink(topic, AT_LEAST_ONCE);
    }

}
