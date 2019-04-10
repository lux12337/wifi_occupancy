import { readFileSync } from 'fs';
import { Config } from './Config';

const [
  ,
  ,
  config_filepath
] = process.argv;

// take configuration parameters from config.json
export const config: Config = JSON.parse(readFileSync(
  config_filepath, 'utf8'
));
