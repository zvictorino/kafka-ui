package io.kafbat.ui.service.user;

import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.acl.AclsService;
import java.util.Collection;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class KafkaUsersService {
  private static final String PRINCIPAL_PREFIX = "User:";

  private final AdminClientKafkaUserCredentialsService adminClientCredentialsService;
  private final ZooKeeperKafkaUserCredentialsService zooKeeperService;
  private final AclsService aclsService;
  private final AdminClientService adminClientService;

  public Flux<KafkaUser> listUsers(KafkaCluster cluster) {
    return listUsers(cluster, false);
  }

  public Flux<KafkaUser> listUsers(KafkaCluster cluster, boolean showSystemUsers) {
    Flux<KafkaUser> users = serviceFor(cluster).listUsers(cluster);
    if (showSystemUsers) {
      // The super user set is only needed to hide those principals, so skip the per-broker
      // broker-config lookups entirely when they are going to be returned anyway.
      return users;
    }
    return users.collectList()
        .zipWith(adminClientService.get(cluster).flatMap(client -> client.loadSuperUsers()))
        .flatMapMany(tuple -> Flux.fromIterable(tuple.getT1().stream()
            .filter(user -> !tuple.getT2().contains(principalFor(user.username())))
            .toList()));
  }

  public Mono<Void> upsertUser(KafkaCluster cluster, String username, String password, Collection<String> mechanisms) {
    if (!StringUtils.hasText(username)) {
      return Mono.error(new ValidationException("Kafka username must not be blank"));
    }
    if (!StringUtils.hasText(password)) {
      return Mono.error(new ValidationException("Kafka user password must not be blank"));
    }
    if (mechanisms == null || mechanisms.isEmpty()) {
      return Mono.error(new ValidationException("At least one SCRAM mechanism must be selected"));
    }
    String principal = principalFor(username);
    return adminClientService.get(cluster)
        .flatMap(client -> client.loadSuperUsers())
        .flatMap(superUsers -> {
          if (superUsers.contains(principal)) {
            return Mono.error(new ValidationException("Kafka super user cannot be modified: " + principal));
          }
          return serviceFor(cluster).upsertUser(cluster, username, password, mechanisms);
        });
  }

  public Mono<Void> deleteCredential(KafkaCluster cluster, String username, String mechanism) {
    return serviceFor(cluster).deleteCredential(cluster, username, mechanism);
  }

  /**
   * A Kafka principal's ACLs outlive its SCRAM credentials. Deleting a user must therefore remove
   * both resources together; otherwise the next credential created for the same name inherits stale access.
   *
   * <p>ACLs are removed first on purpose. If the credential step then fails, the user is left without
   * access but still visible in the listing, so the deletion can simply be retried. Removing the
   * credentials first would instead leave live ACLs behind for a user that no longer appears anywhere,
   * which is exactly the stale-access hazard this method exists to prevent.
   */
  public Mono<Void> deleteUser(KafkaCluster cluster, String username) {
    if (!StringUtils.hasText(username)) {
      return Mono.error(new ValidationException("Kafka username must not be blank"));
    }
    String principal = principalFor(username);
    return adminClientService.get(cluster)
        .flatMap(client -> client.loadSuperUsers())
        .flatMap(superUsers -> {
          if (superUsers.contains(principal)) {
            return Mono.error(new ValidationException("Kafka super user cannot be deleted: " + principal));
          }
          return aclsService.deleteAclsForPrincipal(cluster, principal)
              .then(serviceFor(cluster).deleteUserCredentials(cluster, username));
        });
  }

  private static String principalFor(String username) {
    return PRINCIPAL_PREFIX + username;
  }

  private KafkaUserCredentialsService serviceFor(KafkaCluster cluster) {
    return StringUtils.hasText(cluster.getZookeeperConnect()) ? zooKeeperService : adminClientCredentialsService;
  }
}
