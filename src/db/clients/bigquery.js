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
        disconnect: () => disconnect(client),
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
// 		queryTable = schema+'.'+queryTable;
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
// 		queryTable = schema+'.'+queryTable;
// 	}
	let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function(data) {
		return data[0];
	});
	columns = columns.schema.fields.map(function (x) {return x.name;});
	const query = [
      `INSERT \`${queryTable}\``,
	  `(${columns.join(', ')})`,
      `VALUES (${columns.map(function(x){return '?';}).join(',')});`,
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
	// 	queryTable = schema+'.'+queryTable;
	// }
	let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(function(data) {
		return data[0];
	});
	// 
	columns = columns.schema.fields.map(function (x) {return x.name+'=?';});
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
// 		queryTable = schema+'.'+queryTable;
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
export function disconnect(client) {
  // BigQuery does not have a connection pool. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
	if (client.temptables) {
		cleanTempTables(client, client.temptables);
	}
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
    let views = x[0].filter(function(x) {return x.metadata.type == 'VIEW';});
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
	if (typeof(data) == 'undefined') {
		data = [];
	}
    let response = {fields: Object.keys(data[0] || {}).map((name) => ({ name })),
    command: command,
    rows: data,
    rowCount: data.length};
	if (command === 'UPDATE' || command === 'DELETE' || command === 'INSERT') {
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
	//('has dt or tt', (qt.indexOf('#dt') > 0 || qt.indexOf('#tt') > 0));
    if (qt.indexOf('#dt') > 0 || qt.indexOf('#tt') > 0) {
      //get the table name
      let destOrTemp = 'temp';
      let pos1 = undefined;
	  let writeDisposition = 'WRITE_EMPTY';
	  let splitString = '#dt';
      if (qt.indexOf('#dt') > 0) {
        destOrTemp = 'dest';
        pos1 = qt.indexOf('#dt');
		if (qt.indexOf('#dtwt') > 0) {
			pos1 = qt.indexOf('#dtwt');
			writeDisposition = 'WRITE_TRUNCATE';
			splitString = '#dtwt';
		} else if (qt.indexOf('#dtwa') > 0) {
			pos1 = qt.indexOf('#dtwa');
			writeDisposition = 'WRITE_APPEND';
			splitString = '#dtwa';
		}
      } else {
        pos1 = qt.indexOf('#tt');
		splitString = '#tt';
		if (qt.indexOf('#ttwt') > 0) {
			pos1 = qt.indexOf('#ttwt');
			writeDisposition = 'WRITE_TRUNCATE';
			splitString = '#ttwt';
		} else if (qt.indexOf('#ttwa') > 0) {
			pos1 = qt.indexOf('#ttwa');
			writeDisposition = 'WRITE_APPEND';
			splitString = '#ttwa';
		}
		
      }
      let pos2 = undefined;
      if (qt.indexOf(';') > 0) {
        pos2 = qt.indexOf(';');
      }
      let tableString = qt.substring(pos1,pos2);
      let destTable = tableString.split(splitString)[1].split('.').map(function(x) {return strip(x);});
      queryObject = {
        destination: client.dataset(destTable[0]).table(destTable[1]),
        query: queryText,
        useLegacySql: false,
		writeDisposition: writeDisposition,
      }
      hasDestination = true;
      //('has a destination table', destTable);
    }
    let data = [];
    if (!hasDestination) {
		// //('starting query normally');
    return new Promise((resolve, reject) => {
      client.query(queryObject, function(err, rows) {
        if (err) {
            return reject(err)
		}
        resolve(parseQueryResults(rows, command));
      });
    });
  } else {
    // //('starting query', queryObject);
    return new Promise((resolve, reject)  => {
      client.startQuery(queryObject, function(err, job) {
        if (err) {
			//(err);
          return reject(err); 
        }
        job.getQueryResults(function(err, rows) {
          //('gettingResults');
          resolve(parseQueryResults(rows, command));
        });
      });
    });
  }
}

async function cleanTempTables(client, temptables) {
  //('cleaning temptables');
  sleep(30000).then(() => {
  //('clean wait over');
  for (let i = 0; i < temptables.length; i++) {
    //('cleaning ', temptables[i]);
    //(typeof(temptables[i]))
	let splitString = '#tt';
	if (temptables[i].indexOf('#ttwt') > 0) {
		splitString = '#ttwt';
	} else if (temptables[i].indexOf('#ttwa') > 0) {
		splitString = '#ttwa';
	}
    let tablename = strip(temptables[i].split(splitString)[1]);
    let ds = tablename.split('.')[0];
    let t = tablename.split('.')[1];
    // await _pollForTable(client, temptables[i]);
    //('deleting ', ds, t);
    client.dataset(ds).table(t).delete(function(err, apiResponse) {
      if (err) {
        //(err);
      }
    })
  }});
}

function sleep (time) {
	//sleep function
  return new Promise((resolve) => setTimeout(resolve, time));
}

export async function _pollForTable(client, table) {
	//('starting poll', table);
	//('table defined', (typeof(table) != 'undefined'));
	let splitString = '#tt';
	if (table.indexOf('#ttwt') >= 0) {
		splitString = '#ttwt';
	} 
	if (table.indexOf('#ttwa') >= 0) {
		splitString = '#ttwa';
	} 
	if (table.indexOf('#dt') >= 0) {
		splitString = '#dt';
	} 
	if (table.indexOf('#dtwa') >= 0) {
		splitString = '#dtwa';
	} 
	if (table.indexOf('#dtwt') >= 0) {
		splitString = '#dtwt';
	}
	//("splitstring", splitString);
	let destTable = table.split(splitString)[1].split('.').map(function(x) {return strip(x);});
	let ds = destTable[0];
	let t = destTable[1];
	//('filter on', t);
	let ttExists = undefined;
  try {
    ttExists = await client.dataset(ds).getTables().then(function(x) {
      return x[0].map(function(y){return y.id}).filter(function(y) {return y == t});
    });
  }
  catch (err) {
	  //('API error in poll');
	  //(err);
  }
	while (ttExists.length == 0) {
		sleep(2000).then(() => {});
    try {
      ttExists = await client.dataset(ds).getTables().then(function(x) {
        return x[0].map(function(y){return y.id}).filter(function(y) {return y == t});
      });
    }
    catch (err) {
      //('API error in poll')
    }

	}
}

async function executeQuery(client, queryText) {
  // 
  // 
  // 
  // set the project to the default project ID
  // client.projectId = client.defaultProject;
  // 
  //('querytext', queryText);
  const commands = identifyCommands(queryText);
  //('incoming commands', commands);
  let temptables = [];
  let results = [];
  
  for (let i = 0; i < commands.length; i++) {
    let qt = commands[i].text.toLowerCase();
	let hasTT = false;
	let hasDT = false;
	let tableString = '';
    if (qt.indexOf('#tt') >= 0) {
		//("in tt block");
      let pos1 = qt.indexOf('#tt');
  	  let splitString = '#tt';
      let pos2 = undefined;
      if (qt.indexOf(';') > 0) {
        pos2 = qt.indexOf(';');
      }
	  //("table from", pos1, pos2);
      tableString = qt.substring(pos1,pos2);
	  //(tableString);
      temptables.push(tableString);
	  hasTT = true;
    } else if (qt.indexOf('#dt') >= 0) {
		//sometimes we need to wait on a permanent table too
    	hasDT = true;
        let pos1 = qt.indexOf('#dt');
    	  let splitString = '#dt';
        let pos2 = undefined;
        if (qt.indexOf(';') > 0) {
          pos2 = qt.indexOf(';');
        }
        tableString = qt.substring(pos1,pos2);
  	  //("dt to poll", tableString);
		
    }
	// //("hasTT", hasTT)
	// //("temp tables", temptables);
	  let thisResult = executeSingleQuery(client, commands[i].text, commands[i].type);
	  results.push(thisResult);
	  if (hasTT) {
		  await _pollForTable(client, temptables[i]);
	  } else if (hasDT) {
		  //("polling for dt", tableString);
	  	  await _pollForTable(client, tableString);
	  }
  }
  // //('done with queries');
  // if (temptables.length > 0) {
  //   cleanTempTables(client, temptables);
  // }
  if (temptables.length > 0) {
 	 client.temptables = temptables;
  }
  return Promise.all(results);
  
}

function identifyCommands(queryText) {
  let commands = [];
  try {
    if (queryText.match(/with/i)) {
      //('matches');
      let possibleQueries = queryText.split(';').map(function(x) {return strip(x);});
      for (let i = 0; i < possibleQueries.length; i++) {
        //('evalutating:', possibleQueries[i]);
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
        //(commands);
    }
  } else {
    return identify(queryText);
  }
  } catch (err) {}
    return commands;
}

/*eslint-disable */