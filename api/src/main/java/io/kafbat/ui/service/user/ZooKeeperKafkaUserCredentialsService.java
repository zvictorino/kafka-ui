package io.kafbat.ui.service.user;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.exception.CustomBaseException;
import io.kafbat.ui.exception.NotFoundException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.ScramMechanisms;
import jakarta.annotation.PreDestroy;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.zookeeper.CreateMode;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * SCRAM credentials for ZooKeeper-mode Kafka are dynamic user configs under /config/users.
 * This mirrors Kafka's ConfigCommand: preserve existing user configs, update the selected SCRAM
 * mechanisms and append a /config/changes notification so running brokers reload the credentials.
 */
@Service
@RequiredArgsConstructor
public class ZooKeeperKafkaUserCredentialsService implements KafkaUserCredentialsService {
  private static final String USERS_PATH = "/config/users";
  private static final String CHANGES_PATH = "/config/changes/config_change_";

  private final ObjectMapper objectMapper;
  // One session per cluster. Building a client per request paid a full TCP + ZooKeeper handshake on
  // every list/upsert/delete; the key is the connect string, so reconfiguring a cluster adds a new
  // entry rather than mutating an in-use client.
  private final Map<String, CuratorFramework> clients = new ConcurrentHashMap<>();

  @Override
  public Flux<KafkaUser> listUsers(KafkaCluster cluster) {
    return Mono.fromCallable(() -> withClient(cluster, client -> {
      if (client.checkExists().forPath(USERS_PATH) == null) {
        return List.<KafkaUser>of();
      }
      return client.getChildren().forPath(USERS_PATH).stream()
          .map(sanitizedName -> new KafkaUserConfig(sanitizedName, readConfig(client, sanitizedName)))
          .filter(user -> !scramMechanisms(user.config()).isEmpty())
          .map(user -> new KafkaUser(
              Sanitizer.desanitize(user.sanitizedUsername()),
              scramMechanisms(user.config())
          ))
          .toList();
    }))
        .subscribeOn(Schedulers.boundedElastic())
        .flatMapMany(Flux::fromIterable);
  }

  @Override
  public Mono<Void> upsertUser(KafkaCluster cluster, String username, String password, Collection<String> mechanisms) {
    return Mono.fromRunnable(() -> withClient(cluster, client -> {
      String sanitizedUsername = Sanitizer.sanitize(username);
      Map<String, Object> config = readConfig(client, sanitizedUsername);
      for (String mechanismValue : mechanisms) {
        ScramMechanism mechanism = mechanism(mechanismValue);
        try {
          ScramFormatter formatter = new ScramFormatter(internalMechanism(mechanism));
          config.put(mechanism.mechanismName(), ScramCredentialUtils.credentialToString(
              formatter.generateCredential(password, ScramMechanisms.minimumIterations(mechanism))
          ));
        } catch (Exception e) {
          throw new ValidationException("Unable to generate " + mechanism.mechanismName() + " credential", e);
        }
      }
      writeConfigAndNotify(client, sanitizedUsername, config);
      return null;
    })).subscribeOn(Schedulers.boundedElastic()).then();
  }

  @Override
  public Mono<Void> deleteCredential(KafkaCluster cluster, String username, String mechanismValue) {
    return Mono.fromRunnable(() -> withClient(cluster, client -> {
      String sanitizedUsername = Sanitizer.sanitize(username);
      String mechanismName = mechanism(mechanismValue).mechanismName();
      Map<String, Object> config = readConfig(client, sanitizedUsername);
      if (!config.containsKey(mechanismName)) {
        throw new NotFoundException("Kafka SCRAM credential not found for user " + username);
      }
      config.remove(mechanismName);
      writeConfigAndNotify(client, sanitizedUsername, config);
      return null;
    })).subscribeOn(Schedulers.boundedElastic()).then();
  }

  @Override
  public Mono<Void> deleteUserCredentials(KafkaCluster cluster, String username) {
    return Mono.fromRunnable(() -> withClient(cluster, client -> {
      String sanitizedUsername = Sanitizer.sanitize(username);
      Map<String, Object> config = readConfig(client, sanitizedUsername);
      Set<ScramMechanism> mechanisms = scramMechanisms(config);
      if (mechanisms.isEmpty()) {
        throw new NotFoundException("Kafka SCRAM user not found: " + username);
      }
      mechanisms.forEach(mechanism -> config.remove(mechanism.mechanismName()));
      writeConfigAndNotify(client, sanitizedUsername, config);
      return null;
    })).subscribeOn(Schedulers.boundedElastic()).then();
  }

  private <T> T withClient(KafkaCluster cluster, ZooKeeperOperation<T> operation) {
    String connectString = cluster.getZookeeperConnect();
    if (!StringUtils.hasText(connectString)) {
      throw new ValidationException("zookeeperConnect is required for ZooKeeper-mode Kafka user management");
    }
    CuratorFramework client = clients.computeIfAbsent(connectString, cs -> {
      CuratorFramework newClient = CuratorFrameworkFactory.newClient(cs, new RetryOneTime(1000));
      newClient.start();
      return newClient;
    });
    try {
      if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
        // Drop the pooled session so the next request starts a fresh one instead of hitting the
        // same dead client every time.
        evict(connectString);
        throw new ValidationException("Timed out connecting to ZooKeeper for Kafka user management");
      }
      return operation.execute(client);
    } catch (CustomBaseException e) {
      // NotFoundException raised by the operations above must keep its own 404 error code rather
      // than being re-wrapped as a validation failure.
      throw e;
    } catch (Exception e) {
      throw new ValidationException("Unable to manage Kafka SCRAM credentials through ZooKeeper", e);
    }
  }

  @PreDestroy
  void closeClients() {
    clients.values().forEach(CuratorFramework::close);
    clients.clear();
  }

  private void evict(String connectString) {
    CuratorFramework client = clients.remove(connectString);
    if (client != null) {
      client.close();
    }
  }

  private Map<String, Object> readConfig(CuratorFramework client, String sanitizedUsername) {
    String path = USERS_PATH + "/" + sanitizedUsername;
    try {
      if (client.checkExists().forPath(path) == null) {
        return new LinkedHashMap<>();
      }
      Map<String, Object> root = objectMapper.readValue(
          client.getData().forPath(path), new TypeReference<>() { }
      );
      Object rawConfig = root.get("config");
      Map<String, Object> config = rawConfig instanceof Map<?, ?> map
          ? new LinkedHashMap<>((Map<String, Object>) map)
          : new LinkedHashMap<>();
      return config;
    } catch (Exception e) {
      throw new ValidationException("Unable to read Kafka user credentials from ZooKeeper", e);
    }
  }

  private void writeConfigAndNotify(CuratorFramework client, String sanitizedUsername, Map<String, Object> config) {
    try {
      Map<String, Object> root = Map.of("version", 1, "config", config);
      byte[] data = objectMapper.writeValueAsBytes(root);
      String path = USERS_PATH + "/" + sanitizedUsername;
      if (client.checkExists().forPath(path) == null) {
        client.create().creatingParentsIfNeeded().forPath(path, data);
      } else {
        client.setData().forPath(path, data);
      }
      Map<String, Object> change = Map.of("version", 2, "entity_path", "users/" + sanitizedUsername);
      client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
          .forPath(CHANGES_PATH, objectMapper.writeValueAsBytes(change));
    } catch (Exception e) {
      throw new ValidationException("Unable to persist Kafka SCRAM credentials in ZooKeeper", e);
    }
  }

  private Set<ScramMechanism> scramMechanisms(Map<String, Object> config) {
    Set<ScramMechanism> result = new LinkedHashSet<>();
    for (ScramMechanism mechanism : ScramMechanism.values()) {
      if (config.containsKey(mechanism.mechanismName())) {
        result.add(mechanism);
      }
    }
    return result;
  }

  private ScramMechanism mechanism(String value) {
    return ScramMechanisms.parse(value);
  }

  private org.apache.kafka.common.security.scram.internals.ScramMechanism internalMechanism(ScramMechanism mechanism) {
    return org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(mechanism.mechanismName());
  }

  private record KafkaUserConfig(String sanitizedUsername, Map<String, Object> config) { }

  @FunctionalInterface
  private interface ZooKeeperOperation<T> {
    T execute(CuratorFramework client) throws Exception;
  }
}
