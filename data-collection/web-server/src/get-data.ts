import { readFileSync } from 'fs';
import { config } from './get-config';

// take configuration parameters from config.json
const pomona_output_csv: string = readFileSync(
  config.pomona_output.filepath, 'utf8'
);

export enum DataFormat {
  // csv string
  csv
}

export function pomona_output( format: DataFormat ): string {
  switch (format) {
    case DataFormat.csv:
      return pomona_output_csv;
  }
}
