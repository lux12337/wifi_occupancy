import * as express from 'express';
import * as kill from 'kill-port';
import { config } from './get-config';
import { router as router1 } from './router1';

// collect command-line arguments
const [
  node_filepath,
  script_filepath,
  // config_filepath
] = process.argv;

// create the express app
const app: express.Express = express();

app.use(config.router1.suburl, router1);

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
