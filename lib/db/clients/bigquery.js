'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports._pollForTable = exports.listViews = exports.listTableColumns = exports.listTables = exports.listDatabases = exports.getTableDeleteScript = exports.getTableUpdateScript = exports.getTableInsertScript = exports.getTableSelectScript = exports.listSchemas = undefined;

let listSchemas = exports.listSchemas = (() => {
  var _ref = _asyncToGenerator(function* (client) {
    const data = yield client.getDatasets().then(function (x) {
      return x[0].map(function (y) {
        return y.id;
      });
    });
    return data;
  });

  return function listSchemas(_x) {
    return _ref.apply(this, arguments);
  };
})();

let getTableSelectScript = exports.getTableSelectScript = (() => {
  var _ref2 = _asyncToGenerator(function* (client, table) {
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    }
    const queryTable = table;
    let columns = yield client.dataset(thisDataset).table(thisTable).getMetadata().then(function (data) {
      return data[0];
    });
    columns = columns.schema.fields.map(function (x) {
      return x.name;
    });
    const query = [`SELECT ${columns.join(', ')}`, `FROM \`${queryTable}\`;`].join(' ');
    return query;
  });

  return function getTableSelectScript(_x2, _x3) {
    return _ref2.apply(this, arguments);
  };
})();

let getTableInsertScript = exports.getTableInsertScript = (() => {
  var _ref3 = _asyncToGenerator(function* (client, table) {
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
    let columns = yield client.dataset(thisDataset).table(thisTable).getMetadata().then(function (data) {
      return data[0];
    });
    columns = columns.schema.fields.map(function (x) {
      return x.name;
    });
    const query = [`INSERT \`${queryTable}\``, `(${columns.join(', ')})`, `VALUES (${columns.map(function (x) {
      return '?';
    }).join(',')});`].join(' ');
    //
    return query;
  });

  return function getTableInsertScript(_x4, _x5) {
    return _ref3.apply(this, arguments);
  };
})();

let getTableUpdateScript = exports.getTableUpdateScript = (() => {
  var _ref4 = _asyncToGenerator(function* (client, table) {
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
    let columns = yield client.dataset(thisDataset).table(thisTable).getMetadata().then(function (data) {
      return data[0];
    });
    columns = columns.schema.fields.map(function (x) {
      return `${x.name}=?`;
    });
    const query = ['UPDATE', `\`${queryTable}\``, `SET ${columns.join(', ')}`, 'WHERE <condition>;'].join(' ');
    //
    return query;
  });

  return function getTableUpdateScript(_x6, _x7) {
    return _ref4.apply(this, arguments);
  };
})();

let getTableDeleteScript = exports.getTableDeleteScript = (() => {
  var _ref5 = _asyncToGenerator(function* (client, table) {
    const queryTable = table;
    const query = ['DELETE', 'FROM', `\`${queryTable}\``, 'WHERE <condition>'].join(' ');
    return query;
  });

  return function getTableDeleteScript(_x8, _x9) {
    return _ref5.apply(this, arguments);
  };
})();

let listDatabases = exports.listDatabases = (() => {
  var _ref6 = _asyncToGenerator(function* (client) {
    return [client.projectId];
  });

  return function listDatabases(_x10) {
    return _ref6.apply(this, arguments);
  };
})();

let listTables = exports.listTables = (() => {
  var _ref7 = _asyncToGenerator(function* (client, dataset) {
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

    const schemas = thisDataset.split(',').map(function (x) {
      return strip(x);
    });
    for (let i = 0; i < schemas.length; i += 1) {
      const data = yield client.dataset(schemas[i]).getTables().then(function (x) {
        return x[0].map(function (y) {
          return { name: y.id };
        });
      });
      for (let j = 0; j < data.length; j += 1) {
        data[j].name = `${schemas[i]}.${data[j].name}`;
      }
      allData = allData.concat(data);
    }

    return allData;
  });

  return function listTables(_x11, _x12) {
    return _ref7.apply(this, arguments);
  };
})();

let listTableColumns = exports.listTableColumns = (() => {
  var _ref8 = _asyncToGenerator(function* (client, table) {
    let thisDataset = '';
    let thisTable = '';
    if (table.indexOf('.') > 0) {
      thisTable = table.split('.')[1];
      thisDataset = table.split('.')[0];
    } else {
      thisTable = table;
    }
    const data = yield client.dataset(thisDataset).table(thisTable).getMetadata().then(function (x) {
      return x[0].schema.fields.map(function (y) {
        return { columnName: y.name,
          dataType: y.type,
          mode: y.mode };
      });
    });
    return data;
  });

  return function listTableColumns(_x13, _x14) {
    return _ref8.apply(this, arguments);
  };
})();

let listViews = exports.listViews = (() => {
  var _ref9 = _asyncToGenerator(function* (client, dataset) {
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

    const schemas = thisDataset.split(',').map(function (x) {
      return strip(x);
    });
    for (let i = 0; i < schemas.length; i += 1) {
      const data = yield client.dataset(schemas[i]).getTables().then(function (x) {
        const views = x[0].filter(function (y) {
          return y.metadata.type === 'VIEW';
        });
        return views.map(function (y) {
          return { name: y.id };
        });
      });

      for (let j = 0; j < data.length; j += 1) {
        data[j].name = `${schemas[i]}.${data[j].name}`;
      }
      allData = allData.concat(data);
    }

    return allData;
  });

  return function listViews(_x15, _x16) {
    return _ref9.apply(this, arguments);
  };
})();

let cleanTempTables = (() => {
  var _ref10 = _asyncToGenerator(function* (client, temptables) {
    // ('cleaning temptables');
    sleep(30000).then(function () {
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
        client.dataset(ds).table(t).delete(function (err, apiResponse) {
          if (err) {
            // (err);
          }
        });
      }
    });
  });

  return function cleanTempTables(_x17, _x18) {
    return _ref10.apply(this, arguments);
  };
})();

let _pollForTable = exports._pollForTable = (() => {
  var _ref11 = _asyncToGenerator(function* (client, table) {
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
    const destTable = table.split(splitString)[1].split('.').map(function (x) {
      return strip(x);
    });
    const ds = destTable[0];
    const t = destTable[1];
    let ttExists;
    try {
      ttExists = yield client.dataset(ds).getTables().then(function (x) {
        return x[0].map(function (y) {
          return y.id;
        }).filter(function (y) {
          return y === t;
        });
      });
    } catch (err) {
      return err;
    }
    while (ttExists.length === 0) {
      sleep(2000).then(function () {});
      try {
        ttExists = yield client.dataset(ds).getTables().then(function (x) {
          return x[0].map(function (y) {
            return y.id;
          }).filter(function (y) {
            return y === t;
          });
        });
      } catch (err) {
        return err;
      }
    }
  });

  return function _pollForTable(_x19, _x20) {
    return _ref11.apply(this, arguments);
  };
})();

let executeQuery = (() => {
  var _ref12 = _asyncToGenerator(function* (client, queryText) {
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
        yield _pollForTable(client, temptables[i]);
      } else if (hasDT) {
        // ("polling for dt", tableString);
        yield _pollForTable(client, tableString);
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
  });

  return function executeQuery(_x21, _x22) {
    return _ref12.apply(this, arguments);
  };
})();

exports.default = function (bqconfig) {
  logger().debug('creating database client %j', bqconfig);

  const projectId = bqconfig.database.split('||')[0];
  const keyFilename = bqconfig.database.split('||')[1];
  const client = bigQueryLib({ projectId, keyFilename });
  return {
    client,
    wrapIdentifier,
    connect: () => connect(),
    disconnect: () => disconnect(client),
    listTables: dataset => listTables(client, dataset),
    listViews: dataset => listViews(client, dataset),
    listRoutines: () => listRoutines(client),
    listTableColumns: (table, schema) => listTableColumns(client, table, schema),
    listTableTriggers: table => listTableTriggers(client, table),
    listTableIndexes: (db, table) => listTableIndexes(client, table),
    listSchemas: () => listSchemas(client),
    getTableReferences: table => getTableReferences(client, table),
    getTableKeys: (db, table) => getTableKeys(client, db, table),
    query: queryText => executeQuery(client, queryText),
    executeQuery: queryText => executeQuery(client, queryText),
    listDatabases: () => listDatabases(client),
    // getQuerySelectTop: (table, limit) => getQuerySelectTop(client, table, limit),
    getTableCreateScript: table => getTableCreateScript(client, table),
    getViewCreateScript: view => getViewCreateScript(client, view),
    getTableSelectScript: table => getTableSelectScript(client, table),
    getTableInsertScript: table => getTableInsertScript(client, table),
    getTableUpdateScript: table => getTableUpdateScript(client, table),
    getTableDeleteScript: table => getTableDeleteScript(client, table),
    getRoutineCreateScript: () => getRoutineCreateScript()
  };
};

exports.wrapIdentifier = wrapIdentifier;
exports.listRoutines = listRoutines;
exports.listTableTriggers = listTableTriggers;
exports.listTableIndexes = listTableIndexes;
exports.getTableReferences = getTableReferences;
exports.getTableKeys = getTableKeys;
exports.connect = connect;
exports.disconnect = disconnect;

var _sqlQueryIdentifier = require('sql-query-identifier');

var _logger = require('../../logger');

var _logger2 = _interopRequireDefault(_logger);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; } /* eslint no-unused-vars: "off", require-yield: "off",
                                                                                                                                                                                                                                                                                                                                                                                                                                                                           no-param-reassign: "off", no-param-reassign: "off", no-underscore-dangle: "off"*/


const bigQueryLib = require('@google-cloud/bigquery');

const logger = (0, _logger2.default)('db:clients:bigquery');

function strip(str) {
  return str.replace(/^\s+|\s+$/g, '');
}

function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}

function listRoutines() {
  return Promise.resolve([]);
}

function listTableTriggers() {
  return Promise.resolve([]);
}
function listTableIndexes() {
  return Promise.resolve([]);
}

function getTableReferences() {
  return Promise.resolve([]);
}

function getTableKeys() {
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

function configDatabase(keyfile, project, database) {
  const config = {
    projectId: project,
    keyFilename: keyfile,
    dataset: database
  };

  return config;
}

function connect() {
  return Promise.resolve();
}
function disconnect(client) {
  // bigQueryLib does not have a connection pool. So we open and close connections
  // for every query request. This allows multiple request at same time by
  // using a different thread for each connection.
  // This may cause connection limit problem. So we may have to change this at some point.
  if (client.temptables) {
    cleanTempTables(client, client.temptables);
  }
  return Promise.resolve();
}

function parseQueryResults(data, command) {
  if (typeof data === 'undefined') {
    data = [];
  }
  const response = { fields: Object.keys(data[0] || {}).map(name => ({ name })),
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
    useLegacySql: false
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
    const destTable = tableString.split(splitString)[1].split('.').map(x => strip(x));
    queryObject = {
      destination: client.dataset(destTable[0]).table(destTable[1]),
      query: queryText,
      useLegacySql: false,
      writeDisposition
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

function sleep(time) {
  // sleep function
  return new Promise(resolve => setTimeout(resolve, time));
}

function identifyCommands(queryText) {
  const commands = [];
  try {
    if (queryText.match(/with/i)) {
      // ('matches');
      const possibleQueries = queryText.split(';').map(x => strip(x));
      for (let i = 0; i < possibleQueries.length; i += 1) {
        // ('evalutating:', possibleQueries[i]);
        if (possibleQueries[i].match(/^with/i)) {
          commands.push({ start: 0,
            end: 0,
            text: possibleQueries[i],
            type: 'SELECT',
            executionType: 'LISTING' });
        } else {
          const thisQuery = (0, _sqlQueryIdentifier.identify)(possibleQueries[i]);
          if (thisQuery.length > 0) {
            commands.push(thisQuery[0]);
          }
        }
        // (commands);
      }
    } else {
      return (0, _sqlQueryIdentifier.identify)(queryText);
    }
  } catch (err) {
    return err;
  }
  return commands;
}