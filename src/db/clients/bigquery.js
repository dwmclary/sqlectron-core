/*eslint-disable */

const BigQuery = require('@google-cloud/bigquery');
import { identify } from 'sql-query-identifier';
import uriComponent from '@google-cloud/common';

import createLogger from '../../logger';

const logger = createLogger('db:clients:bigquery');

var bqClient = {};

export function createServer(serverConfig) {
	console.log("in BQ create server");
}

export function createConnection(serverConfig) {
	console.log("in BQ createconnection");
}

export default function (bqconfig) {
  // return new Promise(async (resolve, reject) => {
    // const dbConfig = configDatabase(bqconfig.keyfile, bqconfig.project, bqconfig.database);
    logger().debug('creating database client %j', bqconfig);
    const client  = BigQuery(bqconfig.database);
	bqClient = BigQuery(bqconfig.database);
      return{
		    client: client,
        defaultProject: bqconfig.database.projectId,
        wrapIdentifier,
        connect: () => connect(),
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

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}

function configDatabase(keyfile, project, database) {
  const config = {
    projectId: project,
    keyFilename: keyfile,
    dataset: database
  }

  return config;
}

export function connect() {
	return Promise.resolve();
}
export function disconnect() {
  // BigQuery does not have a connection pool. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
  return Promise.resolve();;
}

export async function listDatabases(client) {
  const data = await client.getDatasets().then(function(x) {
	  return x[0].map(function(y){return y.id;});
  });
  return data;
}

export async function listTables(client, dataset) {
  let thisDataset = dataset.schema;
  let thisProject = '';
  if (thisDataset.indexOf(':') >= 0) {
    const newProject = thisDataset.split(':')[0];
    thisDataset = thisDataset.split(':')[1];
    client.projectId = newProject;
  }
  const data  = await client.dataset(thisDataset).getTables().then(function(x) {
    return x[0].map(function(y){return y.id});
  });
  console.log(data);
  return data;
}

export async function executeQuery(client, queryText) {
  console.log("querytext");
  console.log(queryText);
  console.log("client has query");
  console.log(client.query);
  // set the project to the default project ID
  client.projectId = client.defaultProject;
  let query = {
    'query': queryText,
    'useLegacySQL': false,
  }
  const data = await client.query(query).then(function(x) {
    console.log(x);
    return x;});
  console.log(data);
  return data
}

/*eslint-disable */
