package org.server;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.web.bind.annotation.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

  private final DataSource dataSource;

  public TaskController(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @GetMapping("/countKafkaMessages")
  public int countKafkaMessagesInDb(
          @RequestParam String bootstrapServers,
          @RequestParam String topic) {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "task-controller-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    AtomicInteger messageCount = new AtomicInteger(0);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

      try (Connection connection = dataSource.getConnection()) {
        DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);

        records.forEach(record -> {
          String message = record.value();

          int count = context.fetchCount(
                  context.selectFrom("filter_rules")
                          .where("rule_content LIKE ?", "%" + message + "%")
          );
          messageCount.addAndGet(count);
        });
      } catch (Exception e) {
        throw new RuntimeException("Database error", e);
      }
    }

    return messageCount.get();
  }
}
