import "@typespec/openapi";
import "./responses.tsp";

namespace Api;

using TypeSpec.Http;
using OpenAPI;

@route("/api/clusters/{clusterName}/users")
@tag("KafkaUsers")
interface KafkaUserApi {
  @get
  @operationId("listKafkaUsers")
  @summary("List Kafka SCRAM users")
  listKafkaUsers(@path clusterName: string, @query showSystemUsers?: boolean): KafkaUser[];

  @post
  @operationId("upsertKafkaUser")
  @summary("Create a Kafka SCRAM user or reset credentials for selected mechanisms")
  upsertKafkaUser(
    @path clusterName: string,
    @body payload: UpsertKafkaUser,
  ): void | ApiBadRequestResponse;

  @route("/{username}")
  @delete
  @operationId("deleteKafkaUser")
  @summary("Delete all Kafka SCRAM credentials and ACLs for a user")
  deleteKafkaUser(
    @path clusterName: string,
    @path username: string,
  ): void | ApiNotFoundResponse;

  @route("/{username}/credentials/{mechanism}")
  @delete
  @operationId("deleteKafkaUserCredential")
  @summary("Delete one Kafka SCRAM credential")
  deleteKafkaUserCredential(
    @path clusterName: string,
    @path username: string,
    // Keep this as string: Spring's default path-variable enum converter uses the Java enum name
    // (SCRAM_SHA_512), while the externally visible Kafka mechanism is SCRAM-SHA-512.
    @path mechanism: string,
  ): void | ApiNotFoundResponse;
}

enum ScramMechanism {
  SCRAM_SHA_256: "SCRAM-SHA-256",
  SCRAM_SHA_512: "SCRAM-SHA-512",
}

model KafkaUser {
  username: string;
  mechanisms: ScramMechanism[];
}

model UpsertKafkaUser {
  username: string;
  password: string;
  mechanisms: ScramMechanism[];
}
