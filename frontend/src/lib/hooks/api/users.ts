import { kafkaUsersApiClient as api } from 'lib/api';
import { apiFetch, ServerResponse, showSuccessAlert } from 'lib/errorHandling';
import { ClusterName } from 'lib/interfaces/cluster';
import { KafkaUser, ScramMechanism, UpsertKafkaUser } from 'generated-sources';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

const usersKey = (clusterName: ClusterName) => [
  'clusters',
  clusterName,
  'users',
];

/**
 * ACL principals address SCRAM users as `User:<username>`. Keep this in one place: the Java
 * backend builds the same string, and any divergence leaves ACLs granted to a principal that
 * does not match the user, so deleting the user can never clean them up.
 */
export const kafkaUserPrincipal = (username: string) => `User:${username}`;

export function useKafkaUsers(
  clusterName: ClusterName,
  showSystemUsers = false
) {
  return useQuery<KafkaUser[], ServerResponse>({
    queryKey: [...usersKey(clusterName), { showSystemUsers }],
    queryFn: () =>
      apiFetch(() => api.listKafkaUsers({ clusterName, showSystemUsers })),
  });
}

export function useUpsertKafkaUser(clusterName: ClusterName) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (upsertKafkaUser: UpsertKafkaUser) =>
      api.upsertKafkaUser({ clusterName, upsertKafkaUser }),
    onSuccess: () => {
      showSuccessAlert({ message: 'Kafka user saved successfully' });
      queryClient.invalidateQueries({ queryKey: usersKey(clusterName) });
    },
  });
}

export function useDeleteKafkaUserCredential(clusterName: ClusterName) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      username,
      mechanism,
    }: {
      username: string;
      mechanism: ScramMechanism;
    }) => api.deleteKafkaUserCredential({ clusterName, username, mechanism }),
    onSuccess: () => {
      showSuccessAlert({ message: 'Kafka user credential deleted' });
      queryClient.invalidateQueries({ queryKey: usersKey(clusterName) });
    },
  });
}

export function useDeleteKafkaUser(clusterName: ClusterName) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (username: string) =>
      api.deleteKafkaUser({ clusterName, username }),
    onSuccess: () => {
      showSuccessAlert({ message: 'Kafka user and its ACLs deleted' });
      queryClient.invalidateQueries({ queryKey: usersKey(clusterName) });
      queryClient.invalidateQueries({
        queryKey: ['clusters', clusterName, 'acls'],
      });
    },
  });
}
