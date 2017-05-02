/*eslint-disable */

const BigQuery = require('@google-cloud/bigquery');
import { identify } from 'sql-query-identifier';
import uriComponent from '@google-cloud/common';

import createLogger from '../../logger';

const logger = createLogger('db:clients:bigquery');

export default function (keyfile, project, database) {
  // return new Promise(async (resolve, reject) => {
    const dbConfig = configDatabase(keyfile, project, database);

    logger().debug('creating database client %j', dbConfig);
    const client = BigQuery(dbConfig);
        
    logger().debug('connecting');

      logger().debug('connected');
      return{
        connect: () => connect(dbConfig),
        disconnect: () => disconnect(),
        listTables: (dataset) => listTables(client, dataset),
        listViews: () => listViews(client),
        listRoutines: () => listRoutines(client),
        listTableColumns: (db, table) => listTableColumns(client, db, table),
        listSchemas: () => listSchemas(client),
        getTableReferences: (table) => getTableReferences(client, table),
        query: (queryText) => executeQuery(client, queryText),
        executeQuery: (queryText) => executeQuery(client, queryText),
        listDatabases: () => listDatabases(client),
        getQuerySelectTop: (table, limit) => getQuerySelectTop(client, table, limit),
        getTableCreateScript: (table) => getTableCreateScript(client, table),
        getViewCreateScript: (view) => getViewCreateScript(client, view),
      };
}

function configDatabase(keyfile, project, database) {
  const config = {
    projectId: project,
    keyFile: keyfile,
    dataset: database
  }

  return config;
}

export function connect(config) {
  return BigQuery(config);
}
export function disconnect() {
  // BigQuery does not have a connection pool. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
  return Promise.resolve();;
}

export async function listDatabases(client) {
  console.log(client);
  const result = await client.getDatasets().then(function(x){return x[0].map(function(y){return y.id;});});
  console.log(result);
  return result;
}

export async function listTables(client, dataset) {
  const thisDataset = client.dataset(dataset);
  const { data } = await thisDataset.getTables();

  return data;
}

/*eslint-disable */
