import React, { useState } from 'react';
import { ColumnDef } from '@tanstack/react-table';
import { Action, ResourceType, ScramMechanism } from 'generated-sources';
import ResourcePageHeading from 'components/common/ResourcePageHeading/ResourcePageHeading';
import { Button } from 'components/common/Button/Button';
import PageLoader from 'components/common/PageLoader/PageLoader';
import ErrorPage from 'components/ErrorPage/ErrorPage';
import Table from 'components/common/NewTable';
import { ActionPermissionWrapper } from 'components/common/ActionComponent';
import ActionButton from 'components/common/ActionComponent/ActionButton/ActionButton';
import ClusterContext from 'components/contexts/ClusterContext';
import DeleteIcon from 'components/common/Icons/DeleteIcon';
import PlusIcon from 'components/common/Icons/PlusIcon';
import useAppParams from 'lib/hooks/useAppParams';
import { useConfirm } from 'lib/hooks/useConfirm';
import { ClusterName } from 'lib/interfaces/cluster';
import { DefaultTheme, useTheme } from 'styled-components';
import { useDeleteKafkaUser, useKafkaUsers } from 'lib/hooks/api/users';

import UserForm from './UserForm';
import * as S from './UsersPage.styled';

type KafkaUserRow = { username: string; mechanisms: ScramMechanism[] };

const createColumns = ({
  onPasswordReset,
  onDelete,
  isReadOnly,
  theme,
}: {
  onPasswordReset: (username: string, mechanisms: ScramMechanism[]) => void;
  onDelete: (username: string) => void;
  isReadOnly: boolean;
  theme: DefaultTheme;
}): ColumnDef<KafkaUserRow>[] => [
  { accessorKey: 'username', header: 'Username' },
  {
    id: 'mechanisms',
    header: 'SCRAM mechanisms',
    cell: ({ row }) => row.original.mechanisms.join(', '),
  },
  {
    id: 'password',
    header: 'Password',
    // The click is handled by the wrapper (the inner button carries no onClick), so a disabled
    // read-only button also blocks the permission-wrapped action, exactly like the delete cell.
    cell: ({ row }) => (
      <ActionPermissionWrapper
        onAction={() =>
          onPasswordReset(row.original.username, row.original.mechanisms)
        }
        permission={{ resource: ResourceType.ACL, action: Action.EDIT }}
      >
        <Button buttonType="secondary" buttonSize="S" disabled={isReadOnly}>
          Reset Password
        </Button>
      </ActionPermissionWrapper>
    ),
  },
  {
    // Same as the ACL table: no header, a trash icon at the end of the row shown on hover.
    id: 'delete',
    cell: ({ row }) => (
      <ActionPermissionWrapper
        onAction={() => onDelete(row.original.username)}
        permission={{ resource: ResourceType.ACL, action: Action.EDIT }}
      >
        <S.DeleteCell>
          <div className="show-on-hover">
            <DeleteIcon fill={theme.acl.table.deleteIcon} />
          </div>
        </S.DeleteCell>
      </ActionPermissionWrapper>
    ),
    size: 76,
  },
];

type FormState = {
  editingUsername?: string;
  editingMechanisms?: ScramMechanism[];
};

const UsersPage: React.FC = () => {
  const { clusterName } = useAppParams<{ clusterName: ClusterName }>();
  const { isReadOnly } = React.useContext(ClusterContext);
  const [showSystemUsers, setShowSystemUsers] = useState(false);
  const users = useKafkaUsers(clusterName, showSystemUsers);
  const deleteUser = useDeleteKafkaUser(clusterName);
  // undefined = closed; otherwise the drawer is open for create or password reset.
  const [formState, setFormState] = useState<FormState>();
  const confirm = useConfirm(true);
  const theme = useTheme();

  const closeForm = () => setFormState(undefined);

  const confirmDelete = (username: string) => {
    confirm(
      <>
        Delete Kafka user <strong>{username}</strong>? This permanently removes
        all SCRAM credentials and all ACL rules for{' '}
        <strong>User:{username}</strong>.
      </>,
      () => deleteUser.mutateAsync(username)
    );
  };

  const columns = React.useMemo(
    () =>
      createColumns({
        onPasswordReset: (editingUsername, editingMechanisms) =>
          setFormState({ editingUsername, editingMechanisms }),
        onDelete: confirmDelete,
        isReadOnly,
        theme,
      }),
    [isReadOnly, theme]
  );

  return (
    <S.Container>
      <ResourcePageHeading text="Kafka Users">
        <ActionButton
          buttonType="primary"
          buttonSize="M"
          onClick={() => setFormState({})}
          disabled={isReadOnly}
          permission={{ resource: ResourceType.ACL, action: Action.EDIT }}
        >
          <PlusIcon /> Create User
        </ActionButton>
      </ResourcePageHeading>
      <S.ControlPanel>
        <label>
          <input
            type="checkbox"
            checked={showSystemUsers}
            onChange={(event) => setShowSystemUsers(event.target.checked)}
          />{' '}
          Show system and super users
        </label>
      </S.ControlPanel>

      {users.isLoading && <PageLoader offsetY={300} />}

      {users.error && (
        <ErrorPage
          offsetY={300}
          status={users.error.status}
          text={users.error.message}
          onClick={users.refetch}
        />
      )}

      {users.isSuccess && (
        <Table
          columns={columns}
          data={users.data}
          emptyMessage="No Kafka SCRAM users found"
          enableSorting
        />
      )}

      {formState && (
        <UserForm
          clusterName={clusterName}
          editingUsername={formState.editingUsername}
          editingMechanisms={formState.editingMechanisms}
          onClose={closeForm}
        />
      )}
    </S.Container>
  );
};

export default UsersPage;
