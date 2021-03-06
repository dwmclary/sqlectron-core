'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.removeById = exports.update = exports.add = exports.getAll = undefined;

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

let getAll = exports.getAll = (() => {
  var _ref = _asyncToGenerator(function* () {
    const result = yield config.get();
    return result.servers;
  });

  return function getAll() {
    return _ref.apply(this, arguments);
  };
})();

let add = exports.add = (() => {
  var _ref2 = _asyncToGenerator(function* (server) {
    const srv = _extends({}, server);
    yield (0, _server.validate)(srv);

    const data = yield config.get();
    const newId = _uuid2.default.v4();
    (0, _server.validateUniqueId)(data.servers, newId);

    srv.id = newId;
    data.servers.push(srv);
    yield config.save(data);

    return srv;
  });

  return function add(_x) {
    return _ref2.apply(this, arguments);
  };
})();

let update = exports.update = (() => {
  var _ref3 = _asyncToGenerator(function* (server) {
    yield (0, _server.validate)(server);

    const data = yield config.get();
    (0, _server.validateUniqueId)(data.servers, server.id);

    const index = data.servers.findIndex(function (srv) {
      return srv.id === server.id;
    });
    data.servers = [...data.servers.slice(0, index), server, ...data.servers.slice(index + 1)];

    yield config.save(data);

    return server;
  });

  return function update(_x2) {
    return _ref3.apply(this, arguments);
  };
})();

let removeById = exports.removeById = (() => {
  var _ref4 = _asyncToGenerator(function* (id) {
    const data = yield config.get();

    const index = data.servers.findIndex(function (srv) {
      return srv.id === id;
    });
    data.servers = [...data.servers.slice(0, index), ...data.servers.slice(index + 1)];

    yield config.save(data);
  });

  return function removeById(_x3) {
    return _ref4.apply(this, arguments);
  };
})();

exports.addOrUpdate = addOrUpdate;

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _server = require('./validators/server');

var _config = require('./config');

var config = _interopRequireWildcard(_config);

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

function addOrUpdate(server) {
  const hasId = !!(server.id && String(server.id).length);
  // TODO: Add validation to check if the current id is a valid uuid
  return hasId ? update(server) : add(server);
}