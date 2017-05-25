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

function strip(str) {
    return str.replace(/^\s+|\s+$/g, '');
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
  
  let schemas = thisDataset.split(',').map(function(x) {return strip(x)});
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
  
  let schemas = thisDataset.split(',').map(function(x) {return strip(x)});
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
    let hasDestination = false;
    //check for destination table
    let qt = queryText.toLowerCase();
    if (qt.indexOf("#desttable") > 0 || qt.indexOf("#temptable") > 0) {
      //get the table name
      let destOrTemp = 'temp';
      let pos1 = undefined;
      if (qt.indexOf("#desttable") > 0) {
        destOrTemp = 'dest';
        pos1 = qt.indexOf("#desttable");
      } else {
        pos1 = qt.indexOf("#temptable");
      }
      let pos2 = undefined;
      if (qt.indexOf(";") > 0) {
        pos2 = qt.indexOf(";");
      }
      let tableString = qt.substring(pos1,pos2);
      let destTable = undefined;
      if (destOrTemp === 'dest') {
        destTable = tableString.split("#desttable")[1].split(".").map(function(x) {return strip(x);});
      } else {
        destTable = tableString.split("#temptable")[1].split(".").map(function(x) {return strip(x);});
      }
      queryObject = {
        destination: client.dataset(destTable[0]).table(destTable[1]),
        query: queryText,
        useLegacySql: false,
      }
      hasDestination = true;
      console.log("has a destination table", destTable);
    }
    let data = [];
    if (!hasDestination) {
    return new Promise((resolve, reject) => {
      client.query(queryObject, function(err, rows) {
        if (err) {
			// 
			return reject(err)
		};
        resolve(parseQueryResults(rows, command));
      });
    });
  } else {
    "starting query"
    return new Promise((resolve, reject)  => {
      client.startQuery(queryObject, function(err, job) {
        if (err) {
          return reject(err);
        }
        job.getQueryResults(function(err, rows) {
          console.log("gettingResults");
          resolve(parseQueryResults(rows, command));
        });
      });
    });
  }
}

function cleanTempTables(client, temptables) {
  for (let i = 0; i < temptables.length; i++) {
    let ds = temptables[i].split('.')[0];
    let t = temptables[i].split('.')[1];
    client.dataset(ds).table(t).delete(function(err, apiResponse) {
      if (err) {
        console.log(err);
        return err;
      }
    })
  }
}

function sleep (time) {
	//sleep function
  return new Promise((resolve) => setTimeout(resolve, time));
}

export async function _pollForTable(client, table) {
	console.log("starting poll");
	let destTable = table.split("#temptable")[1].split(".").map(function(x) {return strip(x);});
	let ds = destTable[0];
	let t = destTable[1];
	let ttExists = await client.dataset(ds).getTables().then(function(x) {
      return x[0].map(function(y){return y.id}).filter(function(y) {return y == t});
    });
	console.log("exists before loop", ttExists);
	while (ttExists.length == 0) {
		console.log("sleeping");
		sleep(2000).then(() => {});
			ttExists = await client.dataset(ds).getTables().then(function(x) {
return x[0].map(function(y){return y.id}).filter(function(y) {return y == t});
});

	console.log("exists bottom of loop", ttExists);
	}
}

function executeQuery(client, queryText) {
  // 
  // 
  // 
  // set the project to the default project ID
  // client.projectId = client.defaultProject;
  // 
  console.log("querytext", queryText);
  const commands = identifyCommands(queryText);
  console.log("incoming commands", commands);
  let temptables = [];
  let results = [];
  
  for (let i = 0; i < commands.length; i++) {
    let qt = commands[i].text.toLowerCase();
	let hasTT = false
    if (qt.indexOf("#temptable") > 0) {
      let pos1 = qt.indexOf("#temptable");
      let pos2 = undefined;
      if (qt.indexOf(";") > 0) {
        pos2 = qt.indexOf(";");
      }
      let tableString = qt.substring(pos1,pos2);
      temptables.push(tableString);
	  hasTT = true;
    }

	  let thisResult = executeSingleQuery(client, commands[i].text, commands[i].type);
	  results.push(thisResult);
	  if (hasTT) {
		  _pollForTable(client, temptables[i]);
	  }
  }
  if (temptables.length > 0) {
    cleanTempTables(client, temptables);
  }
  return Promise.all(results);
  
}

function identifyCommands(queryText) {
  let commands = [];
  try {
    if (queryText.match(/with/i)) {
      console.log("matches");
      let possibleQueries = queryText.split(';').map(function(x) {return strip(x);});
      for (let i = 0; i < possibleQueries.length; i++) {
        console.log("evalutating:", possibleQueries[i]);
        if (possibleQueries[i].match(/^with/i)) {
          commands.push({start: 0,
          end: 0,
          text:possibleQueries[i],
          type: 'SELECT', 
          executionType: 'LISTING'})
        } else {
          let thisQuery = identify(possibleQueries[i]);
          if (thisQuery.length > 0) {
            commands.push(thisQuery[0]);
          }
        }
        console.log(commands);
    }
  } else {
    return identify(queryText);
  }
  } catch (err) {}
    return commands;
}

/*eslint-disable */