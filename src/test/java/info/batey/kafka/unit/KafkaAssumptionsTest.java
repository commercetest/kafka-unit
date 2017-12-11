package info.batey.kafka.unit;

import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.CoreMatchers.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class KafkaAssumptionsTest {
    private KafkaUnit kafkaUnitServer;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIntegrationTest.class);

    @Before
    public void setUp() {
        kafkaUnitServer = new KafkaUnit(5000, 5001);
        kafkaUnitServer.setKafkaBrokerConfig("log.segment.bytes", "1024");
        kafkaUnitServer.startup();
    }

    @After
    public void shutdown() throws Exception {
        // The following code checks whether the log size was applied correctly in setUp()
        //   perhaps we could relocate the code into setUp()? TODO try this once the basics work.
        Field f = kafkaUnitServer.getClass().getDeclaredField("broker");
        f.setAccessible(true);
        KafkaServerStartable broker = (KafkaServerStartable) f.get(kafkaUnitServer);
        assertEquals(1024, (int)broker.serverConfig().logSegmentBytes());

        kafkaUnitServer.shutdown();
    }

    @Test
    public void timestampsAreUniqueForOurMessages() throws TimeoutException {
        //given
        String testTopic = "TimestampsNeedToBeUnique";
        kafkaUnitServer.createTopic(testTopic);

        //when
        for (int recordToWrite = 0; recordToWrite < 120; recordToWrite++) {
            // TODO optimize the sending
            ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(testTopic, "key", "value");
            LOGGER.info("sending message:" + recordToWrite + ":" + keyedMessage);
            kafkaUnitServer.sendMessages(keyedMessage);
        }

        List<ConsumerRecord<String, String>> receivedMessages = kafkaUnitServer.readKeyedMessages(testTopic, -1);
        long previousTimestamp = -1L;
        for (int recordToCheck = 0; recordToCheck < 120; recordToCheck++) {
            ConsumerRecord<String, String> message = receivedMessages.get(recordToCheck);
            long currentTimestamp = message.timestamp();
            // TODO add the record it fails on. Also might be worth checking all the records...
            assertThat("Timestamp shouldn't be equal or less than previous value", currentTimestamp, greaterThan(previousTimestamp));
            previousTimestamp = currentTimestamp;
        }
    }
}
