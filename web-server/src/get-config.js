"use strict";
exports.__esModule = true;
var fs_1 = require("fs");
var _a = process.argv, config_filepath = _a[2];
// take configuration parameters from config.json
exports.config = JSON.parse(fs_1.readFileSync(config_filepath, 'utf8'));
