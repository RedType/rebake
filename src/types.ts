import { z } from 'zod';

export const Config = z.object({
  pk: z.string(),
  activeBatch: z.string(),
  pendingBatch: z.string().nullish(),
});
export type Config = z.infer<typeof Config>;

// unfortunately due to a limitation of typescript, recursive types
// can't be statically inferred
// see:
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_AttributeValue.html
export type AttributeValue =
  { B:    string                         } |
  { BS:   string[]                       } |
  { BOOL: boolean                        } |
  { L:    AttributeValue[]               } |
  { M:    Record<string, AttributeValue> } |
  { N:    number                         } |
  { NS:   number[]                       } |
  { NULL: null                           } |
  { S:    string                         } |
  { SS:   string[]                       }
;
//@ts-ignore
export const AttributeValue: z.ZodType<AttributeValue> = z.lazy(() =>
  z.union([
    // base-64 encoded binary
    z.object({ B: z.string() }),
    // base-64 encoded binary set
    z.object({ BS: z.string().array() }),
    // boolean
    z.object({ BOOL:
      z.boolean().or(z.enum(['true', 'false']))
        .transform(b => Boolean(b))
    }),
    // list
    z.object({ L: AttributeValue.array() }),
    // map
    z.object({ M: z.record(z.string(), AttributeValue) }),
    // number
    z.object({ N:
      z.number().or(z.string())
        .transform(n => Number(n))
    }),
    // number set
    z.object({ NS:
      z.number().array().or(z.string().array())
        .transform(ns => ns.map((n: number | string) => Number(n)))
    }),
    // null
    z.object({ NULL: z.literal(null).or(z.string()).transform(() => null) }),
    // string
    z.object({ S: z.string() }),
    // string set
    z.object({ SS: z.string().array() }),
  ])
);

// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html

export const StreamRecord = z.object({
  ApproximateCreationDateTime: z.number(),
  Keys: z.record(z.string(), AttributeValue),
  NewImage: z.record(z.string(), AttributeValue).optional(),
  OldImage: z.record(z.string(), AttributeValue).optional(),
  SequenceNumber: z.string(),
  SizeBytes: z.number(),
  StreamViewType: z.enum([
    'KEYS_ONLY',
    'NEW_IMAGE',
    'OLD_IMAGE',
    'NEW_AND_OLD_IMAGES',
  ]),
});
export type StreamRecord = z.infer<typeof StreamRecord>;

// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html

export const DynamoDBRecord = z.object({
  awsRegion: z.string(),
  dynamodb: StreamRecord,
  eventID: z.string(),
  eventName: z.enum(['INSERT', 'MODIFY', 'REMOVE']),
  eventSource: z.string(),
  eventVersion: z.string(),
  userIdentity: z.object({
    PrincipalId: z.string(),
    Type: z.string(),
  }).optional(),
});
export type DynamoDBRecord = z.infer<typeof DynamoDBRecord>;

// https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html

export const DynamoDBStreamEvent = z.object({ Records: DynamoDBRecord.array() });
export type DynamoDBStreamEvent = z.infer<typeof DynamoDBStreamEvent>;

// https://cloud.google.com/bigquery/docs/reference/rest/v2/tables
export const SchemaType = z.enum([
  'BIGNUMERIC',
  'BOOLEAN', 'BOOL',
  'BYTES',
  'DATE',
  'DATETIME',
  'FLOAT', 'FLOAT64',
  'GEOGRAPHY',
  'INTEGER', 'INT64',
  'NUMERIC',
  'RECORD', 'STRUCT',
  'STRING',
  'TIME',
  'TIMESTAMP',
]);
export type SchemaType = z.infer<typeof SchemaType>;

export const SchemaMode = z.enum(['NULLABLE', 'REPEATED', 'REQUIRED']);
export type SchemaMode = z.infer<typeof SchemaMode>;

// corresponds to type ITableFieldSchema at
// https://github.com/googleapis/nodejs-bigquery/blob/main/src/types.d.ts

export type Schema = {
  categories?: { names?: string[]; };
  collationSpec?: string;
  description?: string;
  fields?: Schema[];
  maxLength?: string;
  mode?: SchemaMode;
  name?: string;
  policyTags?: { names?: string[] };
  precision?: string;
  scale?: string;
  type?: SchemaType;
};
export const Schema: z.ZodType<Schema> = z.lazy(() => z.object({
  categories: z.object({ names: z.string().array() }),
  collationSpec: z.string(),
  description: z.string(),
  fields: Schema.array(),
  maxLength: z.string(),
  mode: SchemaMode,
  name: z.string(),
  policyTags: z.object({ names: z.string().array() }),
  precision: z.string(),
  scale: z.string(),
  type: SchemaType,
}).deepPartial());

