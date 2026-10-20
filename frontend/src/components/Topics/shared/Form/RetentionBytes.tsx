import React from 'react';
import { useFormContext } from 'react-hook-form';
import { ErrorMessage } from '@hookform/error-message';
import { BYTES_IN_GB } from 'lib/constants';
import { InputLabel } from 'components/common/Input/InputLabel.styled';
import Input from 'components/common/Input/Input';
import { FormError } from 'components/common/Input/Input.styled';
import Tooltip from 'components/common/Tooltip/Tooltip';
import InfoIcon from 'components/common/Icons/InfoIcon';

import * as S from './TopicForm.styled';
import RetentionBytesBtns from './RetentionBytesBtns';

const formatBytes = (bytes: number): string => {
  if (bytes === -1) return 'Unlimited';
  if (bytes <= 0) return '';
  if (bytes % BYTES_IN_GB === 0) return `${bytes / BYTES_IN_GB} GB`;
  const gb = bytes / BYTES_IN_GB;
  return `${gb.toFixed(2)} GB`;
};

const RetentionBytes: React.FC = () => {
  const {
    watch,
    formState: { errors },
  } = useFormContext();
  const name = 'retentionBytes';
  const watchedValue = watch(name);

  const valueHint = React.useMemo(() => {
    const value = parseInt(watchedValue, 10);
    if (Number.isNaN(value)) return false;
    return formatBytes(value);
  }, [watchedValue]);

  return (
    <>
      <S.Label>
        <InputLabel htmlFor="topicFormRetentionBytes">
          <S.LabelWithTooltip>
            Max partition size in bytes
            <Tooltip
              value={<InfoIcon />}
              content="Kafka config key: retention.bytes"
              placement="top"
            />
          </S.LabelWithTooltip>
        </InputLabel>
        {valueHint && <span>{valueHint}</span>}
      </S.Label>
      <Input
        id="topicFormRetentionBytes"
        type="number"
        placeholder="Max partition size in bytes (-1 for unlimited)"
        name={name}
        hookFormOptions={{
          min: { value: -1, message: 'must be greater than or equal to -1' },
        }}
        integerOnly
      />
      <FormError>
        <ErrorMessage errors={errors} name={name} />
      </FormError>
      <RetentionBytesBtns name={name} />
    </>
  );
};

export default RetentionBytes;
