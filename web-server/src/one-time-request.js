"use strict";
exports.__esModule = true;
var express = require("express");
var HttpStatusCodes = require("http-status-codes");
var get_config_1 = require("./get-config");
var get_data_1 = require("./get-data");
var subconfig = get_config_1.config.one_time_request;
/**
 * This router will handle requests that ask for the data all at once.
 */
exports.router = express.Router();
exports.router.get('/', function (req, res, next) {
    res.status(HttpStatusCodes.OK).write(get_data_1.pomona_output(get_data_1.DataFormat.csv), 'utf8', function () {
        console.log("pomona_output.csv written into response.");
    });
    res.end(function () {
        console.log("response ended.");
    });
});
