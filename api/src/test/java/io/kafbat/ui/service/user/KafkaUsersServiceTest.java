package io.kafbat.ui.service.user;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ReactiveAdminClient;
import io.kafbat.ui.service.acl.AclsService;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class KafkaUsersServiceTest {

  private final AdminClientKafkaUserCredentialsService adminClientCredentialsService =
      mock(AdminClientKafkaUserCredentialsService.class);
  private final ZooKeeperKafkaUserCredentialsService zooKeeperService =
      mock(ZooKeeperKafkaUserCredentialsService.class);
  private final AclsService aclsService = mock(AclsService.class);
  private final AdminClientService adminClientService = mock(AdminClientService.class);
  private final ReactiveAdminClient reactiveAdminClient = mock(ReactiveAdminClient.class);
  private final KafkaUsersService service = new KafkaUsersService(
      adminClientCredentialsService,
      zooKeeperService,
      aclsService,
      adminClientService
  );

  @BeforeEach
  void setUp() {
    when(adminClientService.get(org.mockito.ArgumentMatchers.any(KafkaCluster.class)))
        .thenReturn(Mono.just(reactiveAdminClient));
    when(reactiveAdminClient.loadSuperUsers()).thenReturn(Mono.just(Set.of()));
  }

  @Test
  void usesZooKeeperServiceWhenZookeeperConnectIsConfigured() {
    KafkaCluster cluster = KafkaCluster.builder().zookeeperConnect("zk-1:2181/kafka").build();
    when(zooKeeperService.listUsers(cluster)).thenReturn(Flux.empty());

    assertThat(service.listUsers(cluster).collectList().block()).isEmpty();

    verify(zooKeeperService).listUsers(cluster);
  }

  @Test
  void usesAdminClientServiceWhenZookeeperConnectIsMissing() {
    KafkaCluster cluster = KafkaCluster.builder().build();
    when(adminClientCredentialsService.listUsers(cluster)).thenReturn(Flux.empty());

    assertThat(service.listUsers(cluster).collectList().block()).isEmpty();

    verify(adminClientCredentialsService).listUsers(cluster);
  }

  @Test
  void delegatesUserUpsertToZooKeeperService() {
    KafkaCluster cluster = KafkaCluster.builder().zookeeperConnect("zk-1:2181/kafka").build();
    when(zooKeeperService.upsertUser(cluster, "app", "secret", List.of("SCRAM-SHA-512")))
        .thenReturn(Mono.empty());

    service.upsertUser(cluster, "app", "secret", List.of("SCRAM-SHA-512")).block();

    verify(zooKeeperService).upsertUser(cluster, "app", "secret", List.of("SCRAM-SHA-512"));
  }

  @Test
  void deletesAclsBeforeCredentialsSoAFailureCannotLeaveStaleAccess() {
    KafkaCluster cluster = KafkaCluster.builder().build();
    when(aclsService.deleteAclsForPrincipal(cluster, "User:app")).thenReturn(Mono.empty());
    when(adminClientCredentialsService.deleteUserCredentials(cluster, "app")).thenReturn(Mono.empty());

    service.deleteUser(cluster, "app").block();

    InOrder inOrder = inOrder(aclsService, adminClientCredentialsService);
    inOrder.verify(aclsService).deleteAclsForPrincipal(cluster, "User:app");
    inOrder.verify(adminClientCredentialsService).deleteUserCredentials(cluster, "app");
  }

  @Test
  void refusesToDeleteSuperUser() {
    KafkaCluster cluster = KafkaCluster.builder().build();
    when(reactiveAdminClient.loadSuperUsers()).thenReturn(Mono.just(Set.of("User:admin")));

    assertThat(catchThrowable(() -> service.deleteUser(cluster, "admin").block()))
        .isInstanceOf(ValidationException.class);

    verifyNoInteractions(aclsService);
    verify(adminClientCredentialsService, never()).deleteUserCredentials(any(), any());
  }

  @Test
  void rejectsBlankUsernameWithValidationError() {
    KafkaCluster cluster = KafkaCluster.builder().build();

    assertThat(catchThrowable(() -> service.upsertUser(cluster, "   ", "secret", List.of("SCRAM-SHA-512")).block()))
        .isInstanceOf(ValidationException.class);
  }
}
