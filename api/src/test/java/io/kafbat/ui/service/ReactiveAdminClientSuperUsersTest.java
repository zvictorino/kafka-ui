package io.kafbat.ui.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

/**
 * Kafka marks {@code super.users} as a sensitive config and redacts it from describeConfigs, so on
 * clusters like that the UI receives an entry carrying no value at all and silently ends up with an
 * empty protected-principal set. These cover the pieces that keep system principals protected and
 * make the situation diagnosable instead.
 */
class ReactiveAdminClientSuperUsersTest {

  /**
   * ConfigEntry/ConfigSynonym have no public constructor taking synonyms, so the shape the broker
   * actually returns is built with mocks.
   */
  private static ConfigEntry superUsersEntry(String value, String synonymValue) {
    var synonym = mock(ConfigEntry.ConfigSynonym.class);
    when(synonym.value()).thenReturn(synonymValue);
    var entry = mock(ConfigEntry.class);
    when(entry.name()).thenReturn("super.users");
    when(entry.value()).thenReturn(value);
    when(entry.synonyms()).thenReturn(List.of(synonym));
    return entry;
  }

  @Test
  void redactedBrokerEntryIsRecognisedAsCarryingNothing() {
    // Exactly what the affected clusters return: sensitive=true, value null, synonym value null.
    var redacted = superUsersEntry(null, null);

    assertThat(ReactiveAdminClient.carriesValue(redacted)).isFalse();
    assertThat(ReactiveAdminClient.superUsersUnavailableReason(2, 2, List.of(redacted)))
        .contains("redacted");
  }

  @Test
  void staticConfigCarriedInSynonymsIsStillUsable() {
    // The shape the original implementation was written for: null value, real value in a synonym.
    var staticConfig = superUsersEntry(null, "User:admin");

    assertThat(ReactiveAdminClient.carriesValue(staticConfig)).isTrue();
    assertThat(ReactiveAdminClient.superUsersUnavailableReason(1, 1, List.of(staticConfig))).isNull();
  }

  @Test
  void configuredPrincipalsAreMergedWithDiscoveredOnes() {
    assertThat(ReactiveAdminClient.mergeWithConfigured(Set.of("User:admin"), Set.of("User:other")))
        .containsExactlyInAnyOrder("User:admin", "User:other");
  }

  @Test
  void configuredPrincipalsSurviveBrokersThatRevealNothing() {
    assertThat(ReactiveAdminClient.mergeWithConfigured(Set.of("User:admin"), Set.of()))
        .containsExactly("User:admin");
  }

  @Test
  void kafkaStyleSemicolonSeparatedValueIsExpandedIntoPrincipals() {
    // How operators actually configure it, following Kafka's own super.users format. Treating the
    // whole value as one principal made the set match nothing and the filter silently do nothing.
    assertThat(ReactiveAdminClient.parseConfiguredSuperUsers(List.of("User:admin;User:other")))
        .containsExactlyInAnyOrder("User:admin", "User:other");
  }

  @Test
  void commaSeparatedValueIsExpandedToo() {
    assertThat(ReactiveAdminClient.parseConfiguredSuperUsers(List.of("User:admin,User:other")))
        .containsExactlyInAnyOrder("User:admin", "User:other");
  }

  @Test
  void onePrincipalPerEntryIsAlsoAccepted() {
    assertThat(ReactiveAdminClient.parseConfiguredSuperUsers(List.of("User:admin", "User:other")))
        .containsExactlyInAnyOrder("User:admin", "User:other");
  }

  @Test
  void paddedAndBlankEntriesAreDropped() {
    assertThat(ReactiveAdminClient.parseConfiguredSuperUsers(List.of(" User:admin ; ", ";User:other;")))
        .containsExactlyInAnyOrder("User:admin", "User:other");
    assertThat(ReactiveAdminClient.parseConfiguredSuperUsers(List.of("", "  ", ","))).isEmpty();
  }

  @Test
  void unreadableBrokerConfigsAreReportedSeparatelyFromRedaction() {
    assertThat(ReactiveAdminClient.superUsersUnavailableReason(0, 3, List.of()))
        .contains("DESCRIBE_CONFIGS");
  }

  @Test
  void clusterWithoutSuperUsersIsNotReportedAsAProblem() {
    assertThat(ReactiveAdminClient.superUsersUnavailableReason(3, 3, List.of())).isNull();
  }
}
