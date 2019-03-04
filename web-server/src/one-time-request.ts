import * as express from 'express';
import * as HttpStatusCodes from 'http-status-codes';
import {config} from './get-config';
import {DataFormat, pomona_output} from './get-data';

const subconfig = config.one_time_request;

/**
 * This router will handle requests that ask for the data all at once.
 */
export const router: express.Router = express.Router();

router.get('/', (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  res.status( HttpStatusCodes.OK ).write(
    pomona_output(DataFormat.csv),
    'utf8',
    () => {
      console.log(`pomona_output.csv written into response.`);
    }
  );

  res.end(() => {
    console.log(`response ended.`);
  });
});
