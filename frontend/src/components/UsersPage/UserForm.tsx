import React, { FC } from 'react';
import { yupResolver } from '@hookform/resolvers/yup';
import { FormProvider, useForm } from 'react-hook-form';
import { mixed, object, ref, string } from 'yup';
import { ScramMechanism, ScramMechanismFromJSON } from 'generated-sources';
import Heading from 'components/common/heading/Heading.styled';
import CloseIcon from 'components/common/Icons/CloseIcon';
import { Button } from 'components/common/Button/Button';
import Input from 'components/common/Input/Input';
import Checkbox from 'components/common/Checkbox/Checkbox';
import ControlledSelect from 'components/common/Select/ControlledSelect';
import ControlledRadio from 'components/common/Radio/ControlledRadio';
import { SelectOption } from 'components/common/Select/Select';
import * as S from 'components/ACLPage/Form/Form.styled';
import { matchTypeOptions } from 'components/ACLPage/Form/constants';
import { MatchType } from 'components/ACLPage/Form/types';
import { showAlert } from 'lib/errorHandling';
import { ClusterName } from 'lib/interfaces/cluster';
import { kafkaUserPrincipal, useUpsertKafkaUser } from 'lib/hooks/api/users';
import { useCreateConsumersAcl, useCreateProducerAcl } from 'lib/hooks/api/acl';

type AccessTemplate = 'NONE' | 'CONSUMER' | 'PRODUCER' | 'READ_WRITE';

type FormValues = {
  username: string;
  password: string;
  passwordConfirmation: string;
  useScram256: boolean;
  useScram512: boolean;
  accessTemplate: AccessTemplate;
  // Like the Custom ACL form, the resource is always typed by hand, so access can be granted to
  // topics and groups that do not exist yet. EXACT takes one or more names (a literal "*" matches
  // everything); PREFIXED takes a name prefix.
  topicsMatchType: MatchType;
  topicsPattern?: string;
  consumerGroupsMatchType: MatchType;
  consumerGroupsPattern?: string;
};

const accessTemplateOptions: SelectOption<AccessTemplate>[] = [
  { label: 'No ACL now', value: 'NONE' },
  { label: 'Read-only consumer', value: 'CONSUMER' },
  { label: 'Producer', value: 'PRODUCER' },
  { label: 'Read and write', value: 'READ_WRITE' },
];

const formSchema = object({
  username: string().trim().required('Username is required'),
  password: string().required('Password is required'),
  passwordConfirmation: string()
    .required('Please confirm the password')
    .oneOf([ref('password')], 'Passwords do not match'),
  accessTemplate: mixed<AccessTemplate>().required(),
  topicsMatchType: mixed<MatchType>().required(),
  topicsPattern: string(),
  consumerGroupsMatchType: mixed<MatchType>().required(),
  consumerGroupsPattern: string(),
});

const emptyValues: FormValues = {
  username: '',
  password: '',
  passwordConfirmation: '',
  useScram256: false,
  useScram512: true,
  accessTemplate: 'NONE',
  topicsMatchType: MatchType.EXACT,
  consumerGroupsMatchType: MatchType.EXACT,
};

type ResourcePattern = { names: string[]; prefix?: string };

const toResourcePattern = (
  matchType: MatchType,
  pattern?: string
): ResourcePattern => {
  const value = pattern?.trim() ?? '';
  if (matchType === MatchType.PREFIXED) {
    return { names: [], prefix: value || undefined };
  }
  return {
    names: value
      .split(',')
      .map((name) => name.trim())
      .filter(Boolean),
  };
};

const isEmpty = ({ names, prefix }: ResourcePattern) =>
  !names.length && !prefix;

export type UserFormProps = {
  clusterName: ClusterName;
  /** Set when resetting the password of an existing user instead of creating one. */
  editingUsername?: string;
  /** Mechanisms the existing user already has, preselected when resetting a password. */
  editingMechanisms?: ScramMechanism[];
  onClose: () => void;
};

const UserForm: FC<UserFormProps> = ({
  clusterName,
  editingUsername,
  editingMechanisms = [],
  onClose,
}) => {
  const upsert = useUpsertKafkaUser(clusterName);
  const createConsumerAcl = useCreateConsumersAcl(clusterName);
  const createProducerAcl = useCreateProducerAcl(clusterName);

  const methods = useForm<FormValues>({
    mode: 'all',
    resolver: yupResolver(formSchema),
    defaultValues: editingUsername
      ? {
          ...emptyValues,
          username: editingUsername,
          useScram256: editingMechanisms.includes(
            ScramMechanismFromJSON('SCRAM-SHA-256')
          ),
          useScram512: editingMechanisms.includes(
            ScramMechanismFromJSON('SCRAM-SHA-512')
          ),
        }
      : emptyValues,
  });

  const accessTemplate = methods.watch('accessTemplate');
  const topicsMatchType = methods.watch('topicsMatchType');
  const consumerGroupsMatchType = methods.watch('consumerGroupsMatchType');
  const isConsumer =
    accessTemplate === 'CONSUMER' || accessTemplate === 'READ_WRITE';
  const isProducer =
    accessTemplate === 'PRODUCER' || accessTemplate === 'READ_WRITE';

  const patternPlaceholder = (matchType: MatchType, noun: string) =>
    matchType === MatchType.PREFIXED
      ? `${noun} prefix, e.g. orders-`
      : `${noun} name(s), comma-separated, or * for all`;

  const submit = async (data: FormValues) => {
    // The username names the SCRAM credential and, prefixed, the ACL principal. The schema trims
    // it, so both always agree: a padded username would otherwise create a user whose ACLs belong
    // to a different principal and can never be cleaned up when the user is deleted.
    const username = data.username.trim();

    const mechanisms = [
      ...(data.useScram256 ? [ScramMechanismFromJSON('SCRAM-SHA-256')] : []),
      ...(data.useScram512 ? [ScramMechanismFromJSON('SCRAM-SHA-512')] : []),
    ];
    if (!mechanisms.length) {
      methods.setError('useScram512', {
        message: 'Select at least one SCRAM mechanism',
      });
      return;
    }

    const topics = toResourcePattern(data.topicsMatchType, data.topicsPattern);
    const groups = toResourcePattern(
      data.consumerGroupsMatchType,
      data.consumerGroupsPattern
    );

    // Everything below is checked before the user is created: a template with nothing to grant
    // access to would otherwise leave a user behind and then fail on the ACL step.
    if ((isConsumer || isProducer) && isEmpty(topics)) {
      methods.setError('topicsPattern', {
        message: 'Enter a topic name or prefix',
      });
      return;
    }
    if (isConsumer && isEmpty(groups)) {
      methods.setError('consumerGroupsPattern', {
        message: 'Enter a consumer group name or prefix',
      });
      return;
    }
    // A prefix is matched literally, so "*" in a prefix is not a wildcard: it would create an ACL
    // for names starting with the character "*", which silently grants nothing.
    if (topics.prefix?.includes('*')) {
      methods.setError('topicsPattern', {
        message: 'A prefix cannot contain *. Use EXACT with * to match all',
      });
      return;
    }
    if (groups.prefix?.includes('*')) {
      methods.setError('consumerGroupsPattern', {
        message: 'A prefix cannot contain *. Use EXACT with * to match all',
      });
      return;
    }

    try {
      await upsert.mutateAsync({
        username,
        password: data.password,
        mechanisms,
      });
    } catch {
      // the global mutation handler already reported the failure
      return;
    }

    const principal = kafkaUserPrincipal(username);
    try {
      if (isConsumer) {
        await createConsumerAcl.createResource({
          principal,
          host: '*',
          topics: topics.names,
          topicsPrefix: topics.prefix,
          consumerGroups: groups.names,
          consumerGroupsPrefix: groups.prefix,
        });
      }
      if (isProducer) {
        await createProducerAcl.createResource({
          principal,
          host: '*',
          topics: topics.names,
          topicsPrefix: topics.prefix,
          idempotent: false,
        });
      }
    } catch {
      // The user already exists by now, so a plain failure would be misleading and would invite a
      // retry that looks like it starts from scratch. The error itself was reported by the global
      // mutation handler; explain the partial state and keep the form open to retry the ACLs.
      showAlert('warning', {
        title: `Kafka user "${username}" was created`,
        message:
          'Assigning ACLs failed. The user already exists - correct the access settings and save again to retry.',
      });
      return;
    }
    onClose();
  };

  return (
    <S.Wrapper data-testid="userForm" $open>
      <Heading level={3}>
        <S.Title>
          {editingUsername ? 'Reset Kafka User Password' : 'Create Kafka User'}
        </S.Title>
        <S.CloseSidebar onClick={onClose} aria-label="Close">
          <CloseIcon />
        </S.CloseSidebar>
      </Heading>
      <S.Content>
        <FormProvider {...methods}>
          <S.Form onSubmit={methods.handleSubmit(submit)}>
            <S.Field>
              <S.Label htmlFor="username">Username</S.Label>
              <Input
                name="username"
                id="username"
                placeholder="Kafka username"
                disabled={Boolean(editingUsername)}
                withError
              />
            </S.Field>
            <S.Field>
              <S.Label htmlFor="password">Password</S.Label>
              <Input
                name="password"
                id="password"
                type="password"
                placeholder="Password"
                autoComplete="new-password"
                withError
              />
            </S.Field>
            <S.Field>
              <S.Label htmlFor="passwordConfirmation">Confirm password</S.Label>
              <Input
                name="passwordConfirmation"
                id="passwordConfirmation"
                type="password"
                placeholder="Confirm password"
                autoComplete="new-password"
                withError
              />
            </S.Field>
            <S.Field>
              <S.Label>SCRAM mechanisms</S.Label>
              <S.ControlList>
                <Checkbox name="useScram512" label="SCRAM-SHA-512" />
                <Checkbox name="useScram256" label="SCRAM-SHA-256" />
              </S.ControlList>
            </S.Field>

            {!editingUsername && (
              <>
                <hr />
                <S.Field>
                  <S.Label htmlFor="accessTemplate">Access template</S.Label>
                  <ControlledSelect
                    name="accessTemplate"
                    options={accessTemplateOptions}
                  />
                </S.Field>
                {(isConsumer || isProducer) && (
                  <S.Field>
                    <S.Label>Topic(s)</S.Label>
                    <S.ControlList>
                      <ControlledRadio
                        name="topicsMatchType"
                        options={matchTypeOptions}
                      />
                      <Input
                        name="topicsPattern"
                        id="topicsPattern"
                        placeholder={patternPlaceholder(
                          topicsMatchType,
                          'Topic'
                        )}
                        withError
                      />
                    </S.ControlList>
                  </S.Field>
                )}
                {isConsumer && (
                  <S.Field>
                    <S.Label>Consumer group(s)</S.Label>
                    <S.ControlList>
                      <ControlledRadio
                        name="consumerGroupsMatchType"
                        options={matchTypeOptions}
                      />
                      <Input
                        name="consumerGroupsPattern"
                        id="consumerGroupsPattern"
                        placeholder={patternPlaceholder(
                          consumerGroupsMatchType,
                          'Consumer group'
                        )}
                        withError
                      />
                    </S.ControlList>
                  </S.Field>
                )}
              </>
            )}
          </S.Form>
        </FormProvider>
      </S.Content>
      <hr />
      <S.Footer>
        <Button buttonSize="M" buttonType="secondary" onClick={onClose}>
          Cancel
        </Button>
        <Button
          buttonSize="M"
          buttonType="primary"
          inProgress={upsert.isPending}
          onClick={methods.handleSubmit(submit)}
        >
          Submit
        </Button>
      </S.Footer>
    </S.Wrapper>
  );
};

export default UserForm;
