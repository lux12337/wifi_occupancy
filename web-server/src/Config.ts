/**
 * Defines the parameters of the config.json file that the server
 * expects.
 */
export interface Config {
  port: number,
  router1: {
    suburl: string
  },
  pomona_output: {
    filepath: string
  }
}
