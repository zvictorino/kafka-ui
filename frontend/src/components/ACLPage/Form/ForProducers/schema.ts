import { array, boolean, object, string } from 'yup';

const formSchema = object({
  username: string().trim().required(),
  host: string().required(),
  topics: array().of(
    object().shape({
      label: string().required(),
      value: string().required(),
    })
  ),
  topicsPrefix: string(),
  transactionalId: string(),
  transactionsIdPrefix: string(),
  idempotent: boolean(),
});

export default formSchema;
