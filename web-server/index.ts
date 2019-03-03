import * as express from 'express';
import * as kill from 'kill-port';
import { readFileSync } from 'fs';
import { Config } from './Config';

const [
  node_filepath,
  script_filepath,
  config_filepath
] = process.argv;

// take configurations from config.json
const config: Config = JSON.parse(readFileSync(
  config_filepath, 'utf8'
));

// create the express app
const app: express.Express = express();

// first, kill the process on the port.
kill( config.port )
.then(() => {
  // then, start the server.
  app.listen( config.port, () => {
    console.log(`
      The server defined in ${script_filepath}
      is now running on port ${config.port}
      using node ${node_filepath}
    `);
  });
})
.catch(() => {
  console.log(`
    failed to kill process at ${config.port}.
  `);
});
