/**
 * Defines the parameters of the config.json file that the server
 * expects.
 */
export interface Config {
  port: number,
  one_time_request: {
    suburl: string
  },
  pomona_output: {
    filepath: string
  }
}
