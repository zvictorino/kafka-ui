package io.kafbat.ui.controller;

import io.kafbat.ui.api.KafkaUsersApi;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaUserDTO;
import io.kafbat.ui.model.ScramMechanismDTO;
import io.kafbat.ui.model.UpsertKafkaUserDTO;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.model.rbac.permission.AclAction;
import io.kafbat.ui.service.mcp.McpTool;
import io.kafbat.ui.service.user.KafkaUser;
import io.kafbat.ui.service.user.KafkaUsersService;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class KafkaUsersController extends AbstractController implements KafkaUsersApi, McpTool {
  private final KafkaUsersService kafkaUsersService;

  @Override
  public Mono<ResponseEntity<Flux<KafkaUserDTO>>> listKafkaUsers(String clusterName, Boolean showSystemUsers,
                                                                  ServerWebExchange exchange) {
    AccessContext context = context(clusterName, AclAction.VIEW, "listKafkaUsers");
    return validateAccess(context)
        .thenReturn(ResponseEntity.ok(kafkaUsersService.listUsers(
            getCluster(clusterName), Boolean.TRUE.equals(showSystemUsers)).map(this::toDto)))
        .doOnEach(sig -> audit(context, sig));
  }

  @Override
  public Mono<ResponseEntity<Void>> upsertKafkaUser(String clusterName, Mono<UpsertKafkaUserDTO> payload,
                                                     ServerWebExchange exchange) {
    AccessContext context = context(clusterName, AclAction.EDIT, "upsertKafkaUser");
    return validateAccess(context)
        .then(payload)
        // An empty body completes the Mono without a value; without this the request would answer
        // 200 having done nothing at all.
        .switchIfEmpty(Mono.error(new ValidationException("Request body is required")))
        .flatMap(user -> kafkaUsersService.upsertUser(
            getCluster(clusterName), user.getUsername(), user.getPassword(),
            // mechanisms is required by the contract, but the generated DTO carries no constraint,
            // so a body omitting it must be rejected by the service instead of throwing an NPE here.
            Optional.ofNullable(user.getMechanisms()).orElse(List.of()).stream()
                .filter(Objects::nonNull)
                .map(ScramMechanismDTO::getValue)
                .toList()))
        .doOnEach(sig -> audit(context, sig))
        .thenReturn(ResponseEntity.ok().build());
  }

  @Override
  public Mono<ResponseEntity<Void>> deleteKafkaUser(String clusterName, String username,
                                                     ServerWebExchange exchange) {
    AccessContext context = context(clusterName, AclAction.EDIT, "deleteKafkaUser");
    return validateAccess(context)
        .then(kafkaUsersService.deleteUser(getCluster(clusterName), username))
        .doOnEach(sig -> audit(context, sig))
        .thenReturn(ResponseEntity.ok().build());
  }

  @Override
  public Mono<ResponseEntity<Void>> deleteKafkaUserCredential(String clusterName, String username,
                                                               String mechanism,
                                                               ServerWebExchange exchange) {
    AccessContext context = context(clusterName, AclAction.EDIT, "deleteKafkaUserCredential");
    return validateAccess(context)
        .then(kafkaUsersService.deleteCredential(getCluster(clusterName), username, mechanism))
        .doOnEach(sig -> audit(context, sig))
        .thenReturn(ResponseEntity.ok().build());
  }

  private AccessContext context(String clusterName, AclAction action, String operationName) {
    return AccessContext.builder().cluster(clusterName).aclActions(action).operationName(operationName).build();
  }

  private KafkaUserDTO toDto(KafkaUser user) {
    return new KafkaUserDTO()
        .username(user.username())
        .mechanisms(user.mechanisms().stream().map(this::toDto).toList());
  }

  private ScramMechanismDTO toDto(ScramMechanism mechanism) {
    return ScramMechanismDTO.fromValue(mechanism.mechanismName());
  }
}
