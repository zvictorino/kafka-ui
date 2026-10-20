import { Option } from 'react-multi-select-component';

export interface FormValues {
  username: string;
  host: string;
  topics?: Option[];
  topicsPrefix?: string;
  transactionalId?: string;
  transactionsIdPrefix?: string;
  idempotent: boolean;
}
