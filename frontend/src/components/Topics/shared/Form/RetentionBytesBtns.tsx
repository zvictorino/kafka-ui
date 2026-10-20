import React from 'react';
import { BYTES_IN_GB } from 'lib/constants';
import styled from 'styled-components';

import RetentionBytesBtn from './RetentionBytesBtn';

export interface Props {
  name: string;
}

const Wrapper = styled.div`
  display: flex;
  gap: 8px;
  padding-top: 8px;
  flex-wrap: wrap;
`;

const RetentionBytesBtns: React.FC<Props> = ({ name }) => (
  <Wrapper>
    <RetentionBytesBtn text="Unlimited" inputName={name} value={-1} />
    <RetentionBytesBtn text="1 GB" inputName={name} value={BYTES_IN_GB} />
    <RetentionBytesBtn text="10 GB" inputName={name} value={BYTES_IN_GB * 10} />
    <RetentionBytesBtn text="20 GB" inputName={name} value={BYTES_IN_GB * 20} />
    <RetentionBytesBtn text="50 GB" inputName={name} value={BYTES_IN_GB * 50} />
    <RetentionBytesBtn
      text="100 GB"
      inputName={name}
      value={BYTES_IN_GB * 100}
    />
  </Wrapper>
);

export default RetentionBytesBtns;
