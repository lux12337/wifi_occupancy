"use strict";
exports.__esModule = true;
var express = require("express");
var kill = require("kill-port");
var get_config_1 = require("./get-config");
var one_time_request_1 = require("./one-time-request");
// collect command-line arguments
var _a = process.argv, node_filepath = _a[0], script_filepath = _a[1];
// create the express app
var app = express();
app.use(get_config_1.config.one_time_request.suburl, one_time_request_1.router);
// first, kill the process on the port.
kill(get_config_1.config.port)
    .then(function () {
    // then, start the server.
    app.listen(get_config_1.config.port, function () {
        console.log("\n      The server defined in " + script_filepath + "\n      is now running on port " + get_config_1.config.port + "\n      using node " + node_filepath + "\n    ");
    });
})["catch"](function () {
    console.log("\n    failed to kill process at " + get_config_1.config.port + ".\n  ");
});
