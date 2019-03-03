import { readFileSync } from 'fs';
import * as express from 'express';

const config: object = JSON.parse(readFileSync(
    './config.json', 'utf8'
));

const app: express.Express = express();

console.log(config);