/*eslint-disable */
import { createConnection } from './client';
import { CLIENTS } from './clients';


export function createServer(serverConfig) {
  console.log("createServer", serverConfig);
  if (!serverConfig) {
    throw new Error('Missing server configuration');
  }

  if (!CLIENTS.some((cli) => cli.key === serverConfig.client)) {
    throw new Error('Invalid SQL client');
  }

  const server = {
    /**
     * All connected dbs
     */
    db: {},

    config: {
      ...serverConfig,
      host: serverConfig.host || serverConfig.socketPath,
    },
  };

  /**
  * Server public API
  */
  return {
    db(dbName) {
      return server.db[dbName];
    },

    end() {
      // disconnect from all DBs
      Object.keys(server.db).forEach((key) => server.db[key].disconnect());

      // close SSH tunnel
      if (server.sshTunnel) {
        server.sshTunnel.close();
        server.sshTunnel = null;
      }
    },

    createConnection(dbName) {

      if (server.db[dbName]) {
        return server.db[dbName];
      }

      const database = {
        database: dbName,
        connection: null,
        connecting: false,
      };
      console.log("database in server createConnection");
      console.log("server", server);
      console.log("database", database);
      console.log("serverconfig", serverConfig);
      console.log(createConnection);
      server.db[dbName] = createConnection(server, database);

      return server.db[dbName];
    },
  };
}
/*eslint-disable */
