import path from 'node:path';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { inspect } from 'node:util';
import zlib from 'node:zlib';
import split2 from 'split2';
import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { BigQuery, Job, JobMetadata } from '@google-cloud/bigquery';

import preprocess from './util/preprocessing';
import getTableName from './util/tables';

/////////////
// CONFIGS //
/////////////

// S3 URI of the data folder
const s3uri = 's3://dynamo-backup-bucket-dw/AWSDynamoDB/01657750284016-2ec55f66/data/';
const [_bqc] = [{
  projectId: 'adhd-dw-core-prod',
  keyFilename: path.join(__dirname, '../gcp_keyfile/prod.json'),
}];
const dataset = 'adhd_dataset_dynamo_prod';
const notTables: string[] = ['rulecollection_state'];
const onlyTables: string[] = ['patients', 'assessment_adhd_results'];
const objectsPerBatch = 3;
const timestamp = '2022-07-13T22:15:27.000Z';

const s3 = new S3Client({});
const bq = new BigQuery().dataset(dataset);

async function main() {
  // extract data from args
  const [_s3, _nothing, Bucket, ...Prefix_] = s3uri.split('/');
  const Prefix = Prefix_.join('/');

  console.log(`Scanning s3://${Bucket}/${Prefix}*`);

  // retrieve keys from bucket
  const listRes = await s3.send(new ListObjectsV2Command({ Bucket, Prefix }));
  const keys = listRes.Contents?.map(o => o.Key) ?? [];

  console.log(`Retrieved ${keys.length} keys: ${inspect(keys)}`);

  // destination streams
  let tables: Record<string, Writable> = {};
  let promises: Promise<void>[] = [];

  // process objects
  let i = 0;
  for (const Key of keys) {
    console.log(`Streaming ${Key}...`);

    // retrieve object stream
    const getObjRes = await s3.send(new GetObjectCommand({
      Bucket, Key,
    }));

    if (!getObjRes.Body || !getObjRes.LastModified) {
      console.error(`  Missing info in response from s3://${Bucket}/${Key}`);
      continue;
    }

    // start streaming
    await pipeline(
      getObjRes.Body as Readable,  // input object stream
      zlib.createGunzip(),         // unzip stream
      split2(),                    // chunk by newlines (individual rows)
      new Writable({               // consume chunks
        write: (chunk, _enc, done) => {
          const record = unmarshall(JSON.parse(chunk).Item);
          const pk = record.pk;
          const sk = record.sk;
          const pksk = JSON.stringify({ pk, sk }); // for easy printing
          const row: any = {
            Keys: { pk, sk },
            Metadata: {
              deleted: false,
              eventKind: 'INSERT',
              processed: 0,
              timestamp: timestamp || getObjRes.LastModified,
            },
          };

          // identify record
          const tableName = getTableName(pk, sk);
          if (!tableName) {
            //console.log(`  Omitting unclassified record ${pksk}`);
            return done();
          }

          // skip non-choice tables
          if (onlyTables.length > 0 && !onlyTables.includes(tableName)) {
            return done();
          } else if (notTables.includes(tableName)) {
            return done();
          }

          // clean record
          const [cleanedRecord, schema] = preprocess('', record, {
            keepEmptyStrings: false, //tableName === 'rulecollection_state'
          }) ?? [];
          if (!cleanedRecord || !schema) {
            console.error(`  Couldn't clean record ${pksk}`);
            return done();
          }

          // add cleaned record to event
          row.NewImage = cleanedRecord;

          // create load stream if missing
          if (!(tableName in tables)) {
            promises.push(new Promise((rs, rj) => {
              const loadStream = tables[tableName] = bq.table(tableName).createWriteStream({
                schema,
                maxBadRecords: Infinity,
                sourceFormat: 'NEWLINE_DELIMITED_JSON',
                createDisposition: 'CREATE_IF_NEEDED',
                writeDisposition: 'WRITE_APPEND',
                ignoreUnknownValues: true,
                schemaUpdateOptions: [
                  'ALLOW_FIELD_ADDITION',
                  'ALLOW_FIELD_RELAXATION',
                ],
              });

              loadStream.on('error', err => {
                console.error(`Error on table ${tableName} load:`, err);
                rj(err);
              });

              loadStream.on('job', (job: Job) => {
                console.log(`Started load job for ${tableName}`);

                job.on('complete', (jm: JobMetadata) => {
                  console.log(`Finished load job for ${tableName}`);
                  console.log(`  Added ${jm.statistics?.load?.outputRows} rows`);
                  const badRecords = jm.statistics?.load?.badRecords;
                  if (badRecords && Number(badRecords) !== 0) {
                    console.warn(`  with ${badRecords} bad records`);
                  }
                  rs();
                });

                job.on('error', err => {
                  console.error(`Error on table ${tableName} load job`)
                  if (err.errors?.get(0)) {
                    const { reason, location } = err.errors?.get(0);
                    console.error(`  ${reason} @ ${location}`);
                  }
                  rj(err);
                });
              });
            }));
          }

          // write record
          const loadStream = tables[tableName];
          if (loadStream.write(JSON.stringify(row) + '\n')) {
            return done();
          } else {
            loadStream.once('drain', done);
          }
        },
      }),
    );

    // after pipeline has been run enough times
    if ((i++ % objectsPerBatch) === (objectsPerBatch - 1)) {
      for (const stream of Object.values(tables)) {
        stream.end();
      }

      // wait for all load jobs to finish
      await Promise.allSettled(promises);

      // reset for next chunk
      tables = {};
      promises = [];
    }
  }

  // finish up last batch
  if (promises.length > 0) {
    for (const stream of Object.values(tables)) {
      stream.end();
    }

    // wait for all load jobs to finish
    await Promise.allSettled(promises);
  }
}

main();

