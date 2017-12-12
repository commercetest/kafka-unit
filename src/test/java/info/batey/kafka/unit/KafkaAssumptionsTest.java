package info.batey.kafka.unit;

import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class KafkaAssumptionsTest {
    private static final int LOG_SEGMENT_BYTES = 131072;
    private static final int RECORDS_TO_PROCESS = 1500;

    private KafkaUnit kafkaUnitServer;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAssumptionsTest.class);

    @Before
    public void setUp() {
        kafkaUnitServer = new KafkaUnit(5000, 5001);
        kafkaUnitServer.setKafkaBrokerConfig(
                "log.segment.bytes", String.valueOf(LOG_SEGMENT_BYTES));
        kafkaUnitServer.setKafkaBrokerConfig("buffer.memory", "33554432");
        kafkaUnitServer.setKafkaBrokerConfig("batch.size", "16384");
        kafkaUnitServer.startup();
    }

    @After
    public void shutdown() throws Exception {
        // The following code checks whether the log size was applied correctly in setUp()
        //   perhaps we could relocate the code into setUp()? TODO try this once the basics work.
        Field f = kafkaUnitServer.getClass().getDeclaredField("broker");
        f.setAccessible(true);
        KafkaServerStartable broker = (KafkaServerStartable) f.get(kafkaUnitServer);
        assertEquals(LOG_SEGMENT_BYTES, (int)broker.serverConfig().logSegmentBytes());

        kafkaUnitServer.shutdown();
    }

    @Test
    public void timestampsAreUniqueForOurMessages() throws TimeoutException {
        //given
        String testTopic = "TimestampsNeedToBeUnique";
        kafkaUnitServer.createTopic(testTopic);

        //when
        for (int recordToWrite = 0; recordToWrite < RECORDS_TO_PROCESS; recordToWrite++) {
            // TODO optimize the sending
            ProducerRecord<String, String> keyedMessage =
                    new ProducerRecord<>(testTopic, "key", "value:" + recordToWrite);
            LOGGER.info("sending message:" + recordToWrite + ":" + keyedMessage);
            kafkaUnitServer.sendMessages(keyedMessage);
        }

        //then try to read all the messages we've written.
        long previousTimestamp = -1L;
        int recordsChecked = 0;
        long timeout = 4500L;
        while (recordsChecked < RECORDS_TO_PROCESS) {
            List<ConsumerRecord<String, String>> receivedMessages =
                kafkaUnitServer.readKeyedMessages(testTopic, -1, timeout);
            for (ConsumerRecord<String, String> message : receivedMessages) {
                long currentTimestamp = message.timestamp();
                LOGGER.info(String.format("Msg[%d] timestamp[%d]", recordsChecked, currentTimestamp));
                // TODO add the record it fails on. Also might be worth checking all the records...
                assertThat("Timestamp shouldn't be equal or less than previous value",
                        currentTimestamp, greaterThan(previousTimestamp));
                assertThat("Timestamp should not be null!", currentTimestamp, not(nullValue()));
                previousTimestamp = currentTimestamp;
                recordsChecked++;
                assertThat("We read more records than we wrote",
                        recordsChecked, lessThanOrEqualTo(RECORDS_TO_PROCESS));
            }
        }
    }
}
