import { Schema } from '../types';
import { tryParseLegacyDate, isIsoUtcDate } from './date';

export interface PreprocessorOpts {
  keepEmptyStrings?: boolean;
}

type PreprocessorOutput = [any, Schema] | null;

// generate a BigQuery schema and do preprocessing on data
const preprocess = (k: string, v: any, opts?: PreprocessorOpts): PreprocessorOutput => {
  k = k.replace(/[^\w\d_]/g, '_');

  switch (typeof v) {
    case 'bigint':
    case 'function':
    case 'symbol':
    case 'undefined':
      throw new Error(`Can't generate schema for a '${typeof v}' (key: ${k})`);

    case 'object':
      // in js, `typeof null` returns 'object'
      // this is a historical language bug that may never be fixed
      if (v === null) {
        return null;
      }

      // process array
      else if (Array.isArray(v)) {
        const allResults = v
          .map((elem, i) => preprocess('' + i, elem, opts))
          .filter(e => !!e) // filter out nulls
        ;

        if (allResults.length <= 0)
          return null;

        const allValues = allResults.map(res => res![0]);
        const allSchemas = allResults.map(res => res![1]);

        // disallow mixed types (allow heterogeneous object types)
        const type = allSchemas.map(s => s.type).reduce((a, s) => a === s ? a : (null as unknown as undefined));
        if (type === null) {
          throw new Error('Array cannot have mixed types');
        }

        // combine schemas
        const schema: Schema = {
          name: k,
          type,
          mode: 'REPEATED',
        };
        if (type === 'RECORD') {
          schema.fields = [];
          const combinedSchemas = allSchemas.flatMap(s => s.fields);
          // deduplicate
          for (const s of combinedSchemas) {
            if (s && !schema.fields.find(d => d.name === s.name)) {
              schema.fields.push(s);
            }
          }
        }

        return [allValues, schema];
      }

      // process pojo
      else {
        return preprocessObject(k, v, opts);
      }

    case 'number':
      return [v, { name: k, type: 'NUMERIC' }];

    case 'boolean':
      return [v, { name: k, type: 'BOOLEAN' }];

    case 'string':
      if (v === '') {
        return opts?.keepEmptyStrings ? [v, { name: k, type: 'STRING' }] : null;
      }

      if (v === 'VOIDED') {
        return null;
      }

      const legacyDate = tryParseLegacyDate(v);

      if (legacyDate !== null)
        return [legacyDate.toISOString(), { name: k, type: 'TIMESTAMP' }];
      else if (isIsoUtcDate(v.trim()))
        return [v.trim(), { name: k, type: 'TIMESTAMP' }];
      else
        return [v, { name: k, type: 'STRING' }];
  }
};

const preprocessObject = (name: string, o: object, opts?: PreprocessorOpts): PreprocessorOutput => {
  const ress = Object.entries(o)
    .map(kv => preprocess(...kv, opts))
    .filter(e => !!e) // remove nulls
  ;

  if (ress.length <= 0)
    return null;

  const obj: Record<string, any> = {};
  const fields = [];
  for (const [elem, schema] of ress as [any, Schema]) {
    obj[schema.name] = elem;
    fields.push(schema);
  }

  return [obj, { name, type: 'RECORD', fields }];
};
export default preprocessObject;

