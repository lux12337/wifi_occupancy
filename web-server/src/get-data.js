"use strict";
exports.__esModule = true;
var fs_1 = require("fs");
var get_config_1 = require("./get-config");
// take configuration parameters from config.json
var pomona_output_csv = fs_1.readFileSync(get_config_1.config.pomona_output.filepath, 'utf8');
var DataFormat;
(function (DataFormat) {
    // csv string
    DataFormat[DataFormat["csv"] = 0] = "csv";
})(DataFormat = exports.DataFormat || (exports.DataFormat = {}));
function pomona_output(format) {
    switch (format) {
        case DataFormat.csv:
            return pomona_output_csv;
    }
}
exports.pomona_output = pomona_output;
