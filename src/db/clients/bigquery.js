/* eslint no-unused-vars: "off", require-yield: "off",
no-param-reassign: "off", no-param-reassign: "off", no-underscore-dangle: "off"*/
import { identify } from 'sql-query-identifier';
import createLogger from '../../logger';


const bigQueryLib = require('@google-cloud/bigquery');

const logger = createLogger('db:clients:bigquery');

export default function (bqconfig) {
  logger().debug('creating database client %j', bqconfig);

  const projectId = bqconfig.database.split('||')[0];
  const keyFilename = bqconfig.database.split('||')[1];
  const client = bigQueryLib({ projectId, keyFilename });
  return {
    client,
    wrapIdentifier,
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
    // getQuerySelectTop: (table, limit) => getQuerySelectTop(client, table, limit),
    getTableCreateScript: (table) => getTableCreateScript(client, table),
    getViewCreateScript: (view) => getViewCreateScript(client, view),
    getTableSelectScript: (table) => getTableSelectScript(client, table),
    getTableInsertScript: (table) => getTableInsertScript(client, table),
    getTableUpdateScript: (table) => getTableUpdateScript(client, table),
    getTableDeleteScript: (table) => getTableDeleteScript(client, table),
    getRoutineCreateScript: () => getRoutineCreateScript(),
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
  const data = await client.getDatasets().then((x) => x[0].map((y) => y.id));
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

export async function getTableSelectScript(client, table) {
  let thisDataset = '';
  let thisTable = '';
  if (table.indexOf('.') > 0) {
    thisTable = table.split('.')[1];
    thisDataset = table.split('.')[0];
  }
  const queryTable = table;
  let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(
      (data) => data[0]);
  columns = columns.schema.fields.map((x) => x.name);
  const query = [
    `SELECT ${columns.join(', ')}`,
    `FROM \`${queryTable}\`;`,
  ].join(' ');
  return query;
}

export async function getTableInsertScript(client, table) {
  let thisDataset = '';
  let thisTable = '';
  if (table.indexOf('.') > 0) {
    thisTable = table.split('.')[1];
    thisDataset = table.split('.')[0];
  }
  const queryTable = table;
    // if (typeof(schema) != 'undefined') {
//      queryTable = schema+'.'+queryTable;
//  }
  let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(
      (data) => data[0]);
  columns = columns.schema.fields.map((x) => x.name);
  const query = [
    `INSERT \`${queryTable}\``,
    `(${columns.join(', ')})`,
    `VALUES (${columns.map((x) => '?').join(',')});`,
  ].join(' ');
    //
  return query;
}

export async function getTableUpdateScript(client, table) {
    //
    //
    //
  let thisDataset = '';
  let thisTable = '';
  if (table.indexOf('.') > 0) {
    thisTable = table.split('.')[1];
    thisDataset = table.split('.')[0];
  }
  const queryTable = table;
  let columns = await client.dataset(thisDataset).table(thisTable).getMetadata().then(
      (data) => data[0]);
  columns = columns.schema.fields.map((x) => `${x.name}=?`);
  const query = [
    'UPDATE',
    `\`${queryTable}\``,
    `SET ${columns.join(', ')}`,
    'WHERE <condition>;',
  ].join(' ');
    //
  return query;
}

export async function getTableDeleteScript(client, table) {
  const queryTable = table;
  const query = [
    'DELETE',
    'FROM',
    `\`${queryTable}\``,
    'WHERE <condition>',
  ].join(' ');
  return query;
}

function configDatabase(keyfile, project, database) {
  const config = {
    projectId: project,
    keyFilename: keyfile,
    dataset: database,
  };

  return config;
}

export function connect() {
  return Promise.resolve();
}
export function disconnect(client) {
  // bigQueryLib does not have a connection pool. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
  if (client.temptables) {
    cleanTempTables(client, client.temptables);
  }
  return Promise.resolve();
}

export async function listDatabases(client) {
  return [client.projectId];
}

export async function listTables(client, dataset) {
  let thisDataset = dataset.schema;
  if (thisDataset.indexOf(':') >= 0) {
    const projectElements = thisDataset.split(':');
    let newProject = projectElements[0];
    if (projectElements.length > 2) {
      newProject = `${projectElements[0]}:${projectElements[1]}`;
    }
    thisDataset = projectElements[projectElements.length - 1];
    client.projectId = newProject;
  }
  let allData = [];

  const schemas = thisDataset.split(',').map((x) => strip(x));
  for (let i = 0; i < schemas.length; i += 1) {
    const data = await client.dataset(schemas[i]).getTables().then(
        (x) => x[0].map((y) => ({ name: y.id })));
    for (let j = 0; j < data.length; j += 1) {
      data[j].name = `${schemas[i]}.${data[j].name}`;
    }
    allData = allData.concat(data);
  }

  return allData;
}

export async function listTableColumns(client, table) {
  let thisDataset = '';
  let thisTable = '';
  if (table.indexOf('.') > 0) {
    thisTable = table.split('.')[1];
    thisDataset = table.split('.')[0];
  } else {
    thisTable = table;
  }
  const data = await client.dataset(thisDataset).table(thisTable).getMetadata().then(
      (x) => x[0].schema.fields.map((y) => ({ columnName: y.name,
        dataType: y.type,
        mode: y.mode })));
  return data;
}

export async function listViews(client, dataset) {
  let thisDataset = dataset.schema;
  if (thisDataset.indexOf(':') >= 0) {
    const projectElements = thisDataset.split(':');
    let newProject = projectElements[0];
    if (projectElements.length > 2) {
      newProject = `${projectElements[0]}:${projectElements[1]}`;
    }
    thisDataset = projectElements[projectElements.length - 1];
    client.projectId = newProject;
  }
  let allData = [];

  const schemas = thisDataset.split(',').map((x) => strip(x));
  for (let i = 0; i < schemas.length; i += 1) {
    const data = await client.dataset(schemas[i]).getTables().then((x) => {
      const views = x[0].filter((y) => y.metadata.type === 'VIEW');
      return views.map((y) => ({ name: y.id }));
    });

    for (let j = 0; j < data.length; j += 1) {
      data[j].name = `${schemas[i]}.${data[j].name}`;
    }
    allData = allData.concat(data);
  }

  return allData;
}

function parseQueryResults(data, command) {
  if (typeof (data) === 'undefined') {
    data = [];
  }
  const response = { fields: Object.keys(data[0] || {}).map((name) => ({ name })),
    command,
    rows: data,
    rowCount: data.length };
  if (command === 'UPDATE' || command === 'DELETE' || command === 'INSERT') {
    response.affectedRows = undefined;
  }
  return response;
}

function executeSingleQuery(client, queryText, command) {
  let queryObject = {
    query: queryText,
    useLegacySql: false,
  };
  let hasDestination = false;
    // check for destination table
  const qt = queryText.toLowerCase();
    // ('has dt or tt', (qt.indexOf('#dt') > 0 || qt.indexOf('#tt') > 0));
  if (qt.indexOf('#dt') > 0 || qt.indexOf('#tt') > 0) {
      // get the table name
    let destOrTemp = 'temp';
    let pos1;
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
    let pos2;
    if (qt.indexOf(';') > 0) {
      pos2 = qt.indexOf(';');
    }
    const tableString = qt.substring(pos1, pos2);
    const destTable = tableString.split(splitString)[1].split('.').map((x) => strip(x));
    queryObject = {
      destination: client.dataset(destTable[0]).table(destTable[1]),
      query: queryText,
      useLegacySql: false,
      writeDisposition,
    };
    hasDestination = true;
      // ('has a destination table', destTable);
  }
  if (!hasDestination) {
        // //('starting query normally');
    return new Promise((resolve, reject) => {
      client.query(queryObject, (err, rows) => {
        if (err) {
          return reject(err);
        }
        resolve(parseQueryResults(rows, command));
      });
    });
  }
    // //('starting query', queryObject);
  return new Promise((resolve, reject) => {
    client.startQuery(queryObject, (err, job) => {
      if (err) {
            // (err);
        return reject(err);
      }
      job.getQueryResults((thisErr, rows) => {
          // ('gettingResults');
        resolve(parseQueryResults(rows, command));
      });
    });
  });
}

async function cleanTempTables(client, temptables) {
  // ('cleaning temptables');
  sleep(30000).then(() => {
  // ('clean wait over');
    for (let i = 0; i < temptables.length; i += 1) {
    // ('cleaning ', temptables[i]);
    // (typeof(temptables[i]))
      let splitString = '#tt';
      if (temptables[i].indexOf('#ttwt') > 0) {
        splitString = '#ttwt';
      } else if (temptables[i].indexOf('#ttwa') > 0) {
        splitString = '#ttwa';
      }
      const tablename = strip(temptables[i].split(splitString)[1]);
      const ds = tablename.split('.')[0];
      const t = tablename.split('.')[1];
    // await _pollForTable(client, temptables[i]);
    // ('deleting ', ds, t);
      client.dataset(ds).table(t).delete((err, apiResponse) => {
        if (err) {
        // (err);
        }
      });
    }
  });
}

function sleep(time) {
    // sleep function
  return new Promise((resolve) => setTimeout(resolve, time));
}

export async function _pollForTable(client, table) {
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
  const destTable = table.split(splitString)[1].split('.').map((x) => strip(x));
  const ds = destTable[0];
  const t = destTable[1];
  let ttExists;
  try {
    ttExists = await client.dataset(ds).getTables().then(
        (x) => x[0].map((y) => y.id).filter((y) => y === t));
  } catch (err) {
    return err;
  }
  while (ttExists.length === 0) {
    sleep(2000).then(() => {});
    try {
      ttExists = await client.dataset(ds).getTables().then(
          (x) => x[0].map((y) => y.id).filter((y) => y === t));
    } catch (err) {
      return err;
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
  // ('querytext', queryText);
  const commands = identifyCommands(queryText);
  // ('incoming commands', commands);
  const temptables = [];
  const results = [];

  for (let i = 0; i < commands.length; i += 1) {
    const qt = commands[i].text.toLowerCase();
    let hasTT = false;
    let hasDT = false;
    let tableString = '';
    if (qt.indexOf('#tt') >= 0) {
        // ("in tt block");
      const pos1 = qt.indexOf('#tt');
      let pos2;
      if (qt.indexOf(';') > 0) {
        pos2 = qt.indexOf(';');
      }
      // ("table from", pos1, pos2);
      tableString = qt.substring(pos1, pos2);
      // (tableString);
      temptables.push(tableString);
      hasTT = true;
    } else if (qt.indexOf('#dt') >= 0) {
        // sometimes we need to wait on a permanent table too
      hasDT = true;
      const pos1 = qt.indexOf('#dt');
      let pos2;
      if (qt.indexOf(';') > 0) {
        pos2 = qt.indexOf(';');
      }
      tableString = qt.substring(pos1, pos2);
      // ("dt to poll", tableString);
    }
    // //("hasTT", hasTT)
    // //("temp tables", temptables);
    const thisResult = executeSingleQuery(client, commands[i].text, commands[i].type);
    results.push(thisResult);
    if (hasTT) {
      await _pollForTable(client, temptables[i]);
    } else if (hasDT) {
          // ("polling for dt", tableString);
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
  const commands = [];
  try {
    if (queryText.match(/with/i)) {
      // ('matches');
      const possibleQueries = queryText.split(';').map((x) => strip(x));
      for (let i = 0; i < possibleQueries.length; i += 1) {
        // ('evalutating:', possibleQueries[i]);
        if (possibleQueries[i].match(/^with/i)) {
          commands.push({ start: 0,
            end: 0,
            text: possibleQueries[i],
            type: 'SELECT',
            executionType: 'LISTING' });
        } else {
          const thisQuery = identify(possibleQueries[i]);
          if (thisQuery.length > 0) {
            commands.push(thisQuery[0]);
          }
        }
        // (commands);
      }
    } else {
      return identify(queryText);
    }
  } catch (err) {
    return err;
  }
  return commands;
}
