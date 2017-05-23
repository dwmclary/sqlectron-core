/*eslint-disable */

const BigQuery = require('@google-cloud/bigquery');
import { identify } from 'sql-query-identifier';
import uriComponent from '@google-cloud/common';

import createLogger from '../../logger';

const logger = createLogger('db:clients:bigquery');

var bqClient = {};

export default function (bqconfig) {

    logger().debug('creating database client %j', bqconfig);
    
    let projectId = bqconfig.database.split('||')[0];
    let keyFilename = bqconfig.database.split('||')[1];
    const client  = BigQuery({projectId: projectId, keyFilename: keyFilename});
	bqClient = BigQuery({projectId: projectId, keyFilename: keyFilename});
      return{
		    client: client,
        wrapIdentifier,
        isBigQuery: () => function() {return true;}, 
        createServer: (serverConfig) => createServer(serverConfig),
        connect: () => connect(),
        disconnect: () => disconnect(),
        listTables: (dataset) => listTables(client, dataset),
        listViews: (dataset) => listViews(client, dataset),
        listRoutines: () => listRoutines(client),
        listTableColumns: (table, schema) => listTableColumns(client, table, schema),
        listTableTriggers: (table) => listTableTriggers(client, table),
        listTableIndexes: (db, table) => listTableIndexes(client, table),
        listSchemas: () => listSchemas(client),
        getTableReferences: (table) => getTableReferences(client, table),
		getTableKeys: (db, table) => getTableKeys(client, db, table),
        query: (queryText) => executeQuery(client, queryText),
        executeQuery: (queryText) => executeQuery(client, queryText),
        listDatabases: () => listDatabases(client),
        getQuerySelectTop: (table, limit) => getQuerySelectTop(client, table, limit),
        getTableCreateScript: (table) => getTableCreateScript(client, table),
        getViewCreateScript: (view) => getViewCreateScript(client, view),
		getTableSelectScript: (table, schema) => getTableSelectScript(client, table, schema),
		getTableInsertScript: (table, schema) => getTableInsertScript(client, table, schema),
		getTableUpdateScript: (table, schema) => getTableUpdateScript(client, table, schema),
		getTableDeleteScript: (table, schema) => getTableDeleteScript(client, table, schema),
        getRoutineCreateScript: (routine) => getRoutineCreateScript(),
      };
}

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}

export function listRoutines() {
  return Promise.resolve([]);
}

export function listTableTriggers() {
  return Promise.resolve([]);
}
export function listTableIndexes() {
  return Promise.resolve([]);
}

export async function listSchemas(client) {
  const data = await client.getDatasets().then(function(x) {
	  return x[0].map(function(y){return y.id;});
  });
  return data;
}

export function getTableReferences() {
  return Promise.resolve([]);
}

export function getTableKeys() {
  return Promise.resolve([]);
}

function getTableCreateScript() {
  return Promise.resolve([]);
}

function getViewCreateScript() {
  return Promise.resolve([]);
}

function getRoutineCreateScript() {
  return Promise.resolve([]);
}

export async function getTableSelectScript(client, table, schema) {
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    }
	let queryTable = table;
	// if (typeof(schema) != 'undefined') {
// 		queryTable = schema+"."+queryTable;
// 	}
	let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function(data) {
		return data[0];
	});
	columns = columns.schema.fields.map(function (x) {return x.name;});
	const query = [
      `SELECT ${columns.join(', ')}`,
      `FROM \`${queryTable}\`;`,
    ].join(' ');
	return query;
}

export async function getTableInsertScript(client, table, schema) {
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    }
	let queryTable = table;
	// if (typeof(schema) != 'undefined') {
// 		queryTable = schema+"."+queryTable;
// 	}
	let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function(data) {
		return data[0];
	});
	columns = columns.schema.fields.map(function (x) {return x.name;});
	const query = [
      `INSERT \`${queryTable}\``,
	  `(${columns.join(', ')})`,
      `VALUES (${columns.map(function(x){return "?";}).join(',')});`,
    ].join(' ');
	// 
	return query;
}

export async function getTableUpdateScript(client, table, schema) {
	// 
	// 
	// 
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    }
	let queryTable = table;
	// if (typeof(schema) != 'undefined') {
	// 	queryTable = schema+"."+queryTable;
	// }
	let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function(data) {
		return data[0];
	});
	// 
	columns = columns.schema.fields.map(function (x) {return x.name+"=?";});
	const query = [
      `UPDATE`,
    '\`'+`${queryTable}`+'\`',
	  `SET ${columns.join(', ')}`,
      `WHERE <condition>;`,
    ].join(' ');
	// 
	return query;
}

export async function getTableDeleteScript(client, table, schema) {
	let queryTable = table;
	// if (typeof(schema) != 'undefined') {
// 		queryTable = schema+"."+queryTable;
// 	}
	const query = [
      `DELETE`,
      'FROM',
    '\`'+`${queryTable}`+'\`',
		`WHERE <condition>`,
    ].join(' ');
	// 
	return query;
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
  // const data = await client.getDatasets().then(function(x) {
//     return x[0].map(function(y){return y.id;});
//   });
//   return data;
return [client.projectId]; 
}

export async function listTables(client, dataset) {
  
  let thisDataset = dataset.schema;
  let thisProject = '';
  if (thisDataset.indexOf(':') >= 0) {
	  const projectElements = thisDataset.split(':');
 	  let newProject = projectElements[0];
	  if (projectElements.length > 2) {
	  	newProject = projectElements[0] + ':' + projectElements[1];
	  }
    thisDataset = projectElements[projectElements.length - 1];
    client.projectId = newProject;
  }
  let all_data = [];
  
  let schemas = thisDataset.split(',');
  for (let i = 0; i< schemas.length; i++) {
    
    const data  = await client.dataset(schemas[i]).getTables().then(function(x) {
      return x[0].map(function(y){return {name: y.id}});
    });
    
    for (let j = 0; j < data.length; j++) {
      data[j].name = schemas[i]+'.'+data[j].name;
    }
    all_data = all_data.concat(data);
  }
  
  return all_data;
}

export async function listTableColumns(client, table, schema) {
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    } else {
      thisTable = table;
    }
    // if (typeof(schema) != 'undefined') {
    //   thisDataset = schema;
    // }
    
	const data = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function (x) {
		return x[0].schema.fields.map(function(x) {
			return {columnName: x.name,
			dataType: x.type,
				mode: x.mode};
		});
	});
	return data;
}

export async function listViews(client, dataset) {
  let thisDataset = dataset.schema;
  let thisProject = '';
  if (thisDataset.indexOf(':') >= 0) {
	  const projectElements = thisDataset.split(':');
 	  let newProject = projectElements[0];
	  if (projectElements.length > 2) {
	  	newProject = projectElements[0] + ':' + projectElements[1];
	  }
    thisDataset = projectElements[projectElements.length - 1];
    client.projectId = newProject;
  } 
  let all_data = [];
  
  let schemas = thisDataset.split(',');
  for (let i = 0; i< schemas.length; i++) {
  const data  = await client.dataset(schemas[i]).getTables().then(function(x) {
    let views = x[0].filter(function(x) {return x.metadata.type == "VIEW";});
    return views.map(function(y){return {name: y.id}});
  });
    
    for (let j = 0; j < data.length; j++) {
      data[j].name = schemas[i]+'.'+data[j].name;
    }
    all_data = all_data.concat(data);
  }
  
  return all_data;
}

function parseQueryResults(data, command) {
    let response = {fields: Object.keys(data[0] || {}).map((name) => ({ name })),
    command: command,
    rows: data,
    rowCount: data.length};
	if (command === "UPDATE" || command === "DELETE" || command === "INSERT") {
		response.affectedRows = undefined;
	}
      return response;
}

function executeSingleQuery(client, queryText, command) {
    let queryObject = {
      query: queryText,
      useLegacySql: false,
    }
    let data = [];
    return new Promise((resolve, reject) => {
      client.query(queryObject, function(err, rows) {
        if (err) {
			// 
			return reject(err)
		};
        resolve(parseQueryResults(rows, command));
      });
    });
}

function executeQuery(client, queryText) {
  // 
  // 
  // 
  // set the project to the default project ID
  // client.projectId = client.defaultProject;
  // 
  const commands = identifyCommands(queryText);
  // 
  let results = [];
  for (var i = 0; i < commands.length; i++) {
	  let thisResult = executeSingleQuery(client, commands[i].text, commands[i].type);
	  results.push(thisResult);
  }
  return Promise.all(results);
  
}

function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}

/*eslint-disable */
