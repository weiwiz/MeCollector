/**
 * Created by jacky on 2017/2/4.
 */
'use strict';
var _ = require('lodash');
var util = require('util');
var async = require('async');
var zookeeper = require('node-zookeeper-client');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
var OPERATION_SCHEMAS = {
    "collect": {
        "type": "object",
        "properties": {
            "uuid": {"type": "string"},
            "items": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        },
        "required": ["uuid", "items"]
    }
};


var getDeviceTypeFromZkPath = function (zkPath) {
    var pathNodes = zkPath.split("/");
    if (pathNodes.length >= 6) {
        return pathNodes[3] + pathNodes[4] + pathNodes[5] + pathNodes[6];
    }
    else {
        return null;
    }
};

var getDeviceCmdCode = function (zkPath) {
    var pathNodes = zkPath.split("/");
    if (pathNodes.length >= 8) {
        return pathNodes[8];
    }
    else {
        return null;
    }
};

var getZkNodeChildren = function (zkClient, zkPath, watch, callback) {
    if (util.isNullOrUndefined(callback)) {
        callback = watch;
        watch = null;
    }
    zkClient.getChildren(zkPath,
        watch,
        function (error, children, stat) {
            if (error) {
                callback({
                    errorId: 214001,
                    errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
                });
            }
            else {
                callback(null, zkPath, children);
            }
        });
};

var getZkNodeData = function (zkClient, zkPath, watch, callback) {
    if (util.isNullOrUndefined(callback)) {
        callback = watch;
        watch = null;
    }
    zkClient.getData(zkPath,
        watch,
        function (error, data, stat) {
            if (error) {
                callback({
                    errorId: 214001,
                    errorMsg: "zhPath=[" + zkPath + "]:" + JSON.stringify(error)
                });
            }
            else {
                var dataStr = data.toString('utf8');
                callback(null, zkPath, dataStr);
            }
        });
};

var collect = function (self, deviceInfo) {
    var collectMessage = {
        devices: self.configurator.getConfRandom("services.executor"),
        payload: {
            cmdName: "execute",
            cmdCode: "0001",
            parameters: {
                userUuid: deviceInfo.userId,
                deviceUuid: deviceInfo.uuid,
                cmd: self.getCollectConf(deviceInfo.type.id).cmd
            }
        }
    };
    // var collectMessage = {
    //     devices: deviceInfo.uuid,
    //     payload: self.getCollectConf(deviceInfo.type.id).cmd
    // };
    // if (deviceInfo.type.id.substr(0, 2) === "04") {
    //     collectMessage = {
    //         devices: deviceInfo.owner,
    //         payload: {
    //             cmdName: "forward",
    //             cmdCode: "0001",
    //             parameters: {
    //                 uuid: deviceInfo.uuid,
    //                 deviceType: deviceInfo.type.id,
    //                 cmd: self.getCollectConf(deviceInfo.type.id).cmd
    //             }
    //         }
    //     };
    // }

    self.message(collectMessage, function (response) {
        if (response.retCode !== 200) {
            logger.error(response.retCode, response.description);
        }
        else {
            var putDataMsg = {
                devices: self.configurator.getConfRandom("services.data_manager"),
                payload: {
                    cmdName: "putData",
                    cmdCode: "0004",
                    parameters: {
                        uuid: deviceInfo.uuid,
                        userId: deviceInfo.userId,
                        type: deviceInfo.type.id,
                        timestamp: new Date().toISOString(),
                        offset: parseInt(deviceInfo.timeZone.offset),
                        data: response.data
                    }
                }
            };
            self.message(putDataMsg, function (response) {
                if (response.retCode !== 200) {
                    logger.error(response.retCode, response.description);
                }
            });
            logger.debug(response.data);
        }
    });
};

function Collector(conx, uuid, token, configurator) {
    this.intervalId = null;
    this.collectMap = [];//[deviceType:"", cmd:{cmdName:"", cmdCode:"", parameters:[]}]
    this.getCollectConf = function (deviceType) {
        var self = this;
        for (var i = 0, len = self.collectMap.length; i < len; ++i) {
            if (self.collectMap[i].deviceType === deviceType) {
                return self.collectMap[i];
            }
        }
    };
    this.collect = function (self) {
        for (var i = 0, mLen = self.collectMap.length; i < mLen; ++i) {
            var mapItem = self.collectMap[i];
            var msg = {
                devices: self.configurator.getConfRandom("services.device_manager"),
                payload: {
                    cmdName: "getDevice",
                    cmdCode: "0003",
                    parameters: {
                        "type.id": mapItem.deviceType
                    }
                }
            };
            self.message(msg, function (response) {
                if (response.retCode !== 200) {
                    //logger.error(response.retCode, response.description);
                }
                else {
                    var devices = response.data;
                    for (var j = 0, dLen = devices.length; j < dLen; ++j) {
                        var deviceInfo = devices[j];
                        collect(self, deviceInfo);
                    }
                }
            })
        }

        /*
        *单独采集月动温控器
        * */
        var message = {
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
                cmdName: "getDevice",
                cmdCode: "0003",
                parameters: {
                    "type.id": "050608070001"
                }
            }
        };
        self.message(message, function (response) {
            if (response.retCode === 200) {
                var devices = response.data;
                for (var j = 0, dLen = devices.length; j < dLen; ++j) {
                    var deviceInfo = devices[j];
                    if (!util.isNullOrUndefined(deviceInfo.userId)   //如果设备处于无主状态，不采集
                        && !util.isNullOrUndefined(deviceInfo.extra)
                        && !util.isNullOrUndefined(deviceInfo.extra.connection) //如果掉线，不采集
                        && !util.isNullOrUndefined(deviceInfo.extra.items)
                        && !util.isNullOrUndefined(deviceInfo.extra.items.dis_temp)
                    ) {
                        var temperature = parseFloat(deviceInfo.extra.items.dis_temp.replace(/c/g,""));
                        var putDataMsg = {
                            devices: self.configurator.getConfRandom("services.analyzer"),
                            payload: {
                                cmdName: "putData",
                                cmdCode: "0004",
                                parameters: {
                                    uuid: deviceInfo.uuid,
                                    userId: deviceInfo.userId,
                                    type: deviceInfo.type.id,
                                    timestamp: new Date().toISOString(),
                                    offset: deviceInfo.timeZone.offset,
                                    data: [{name: "dis_temp", value: temperature}]
                                }
                            }
                        };
                        self.message(putDataMsg, function (response) {
                            if (response.retCode !== 200) {
                                logger.error(response.retCode, response.description);
                            }
                        });
                        logger.debug([{name: "dis_temp", value: temperature}]);
                    }
                }
            }
        });
    };
    VirtualDevice.call(this, conx, uuid, token, configurator);
}
util.inherits(Collector, VirtualDevice);

Collector.prototype.start = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: {}};
    if (!self.intervalId) {
        self.intervalId = setInterval(self.collect, 300 * 1000, self);
    }
    if (!util.isNullOrUndefined(peerCallback) && util.isFunction(peerCallback)) {
        peerCallback(responseMessage);
    }
};
Collector.prototype.stop = function (message, peerCallback) {
    var self = this;
    var responseMessage = {retCode: 200, description: "Success.", data: {}};
    if (self.intervalId) {
        clearInterval(self.intervalId);
        self.intervalId = null;
    }
    if (!util.isNullOrUndefined(peerCallback) && util.isFunction(peerCallback)) {
        peerCallback(responseMessage);
    }
};
Collector.prototype.init = function () {
    var self = this;
    self.start();
    async.waterfall([
            function (innerCallback) {
                var zkPath = "/devices/types";
                getZkNodeChildren(self.configurator.zkClient, zkPath, function (error, path, children) {
                    if (error) {
                        innerCallback(null, []);
                    }
                    else {
                        for (var i = 0, len = children.length; i < len; ++i) {
                            children[i] = path + "/" + children[i];
                        }
                        innerCallback(null, children);
                    }
                });
            },
            function (parent, innerCallback) { //parent = /devices/types/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx/xx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i], function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            },
            function (parent, innerCallback) { //parent = /devices/types/xx/xx/xx/xxxxxx
                var parentCount = parent.length;
                var childrenPath = [];
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeChildren(self.configurator.zkClient, parent[i] + "/commands", function (error, path, children) {
                        if (!error && util.isArray(children)) {
                            for (var j = 0, cLen = children.length; j < cLen; ++j) {
                                childrenPath.push(path + "/" + children[j]);
                            }
                        }
                        if (--parentCount <= 0) {
                            innerCallback(null, childrenPath);
                        }
                    });
                }
            }
        ],
        function (error, parent) {//parent = /devices/types/xx/xx/xx/xxxxxx/commands/xxxx
            if (!error) {
                for (var i = 0, pLen = parent.length; i < pLen; ++i) {
                    getZkNodeData(self.configurator.zkClient
                        , parent[i] + "/collect"
                        , function (error, path, data) {
                            if (!error) {
                                try {
                                    var deviceType = getDeviceTypeFromZkPath(path);
                                    var cmdCode = getDeviceCmdCode(path);
                                    var parameters = JSON.parse(data);
                                    var mapItem = {
                                        deviceType: deviceType,
                                        cmd: {
                                            cmdName: "",
                                            cmdCode: cmdCode,
                                            parameters: parameters
                                        }
                                    };
                                    if (deviceType && cmdCode && parameters) {
                                        var pathCmdName = path.replace(/collect/g, "name");
                                        getZkNodeData(self.configurator.zkClient, pathCmdName, function (error, path, data) {
                                            if (!error) {
                                                mapItem.cmd.cmdName = data;
                                                self.collectMap.push(mapItem);
                                            }
                                        });
                                    }
                                }
                                catch (e) {
                                    logger.error(214000, e);
                                }
                            }
                        })
                }
            }
        });
};

module.exports = {
    Service: Collector,
    OperationSchemas: OPERATION_SCHEMAS
};