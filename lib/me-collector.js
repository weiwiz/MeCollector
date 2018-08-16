/**
 * Created by jacky on 2017/2/4.
 */
'use strict';

var _ = require('lodash');
var util = require('util');
var async = require('async');
var VirtualDevice = require('./virtual-device').VirtualDevice;
var logger = require('./mlogger/mlogger');
var PLANT_ITEMS = [
  ["洞头|乡政府", "136", "74.25 ", "275", "130002509608"],
  ["洞头|敬老院", "60", "59.4", "220", "130002509749"],
  ["洞头|中心小学", "45", "64.53", "239", "130002509641"],
  ["洞头|幼儿园", "35", "43.74", "162", "130002509722"],
  ["洞头|官丰村小学", "45", "39.96", "148", ""],
  ["洞头|洞头乡卫生院", "19", "25.38", "94", "130002509721"],
  ["洞头|河头村烤烟房1", "22", "28.08 ", "104", "130002509617"],
  ["洞头|河头村烤烟房2", "22", "28.08", "104", "130002509699"],
  ["洞头|河头村烤烟房3", "22", "28.08", "104", "130002509744"],
  ["洞头|丰胜村烤烟房1", "44", "54.00 ", "200", "160004450902"],
  ["洞头|丰胜村烤烟房2", "44", "56.16 ", "208", "160004451177"],
  ["洞头|洞下村烤烟房", "50", "27.00 ", "100", ""],
  ["洞头|中心小学中转房", "12", "59.89 ", "207", "130002509752"],
  ["洞头|长教烤烟房", "44", "11.34 ", "42", "160004451915"],
  ["中村|小学", "100", "125.52 ", "476", "130001369501"],
  ["中村|中学", "90", "48.60 ", "180", "160004451978"],
  ["中村|敬老院（2栋）", "30", "32.4", "120", ""],
  ["中村|烤烟房（6栋）", "80", "99.36 ", "368", ""],
  ["站塘|站塘乡敬老院", "40", "65.34 ", "242", "150001280193"],
  ["站塘|初中", "120", "90.18 ", "334", "160004451040"],
  ["站塘|社山坝小学", "45", "31.32", "116", "160004451039"],
  ["站塘|保障房旁烤烟房", "25", "25.38 ", "94", "160004752368"],
  ["站塘|安然纳米小学", "50", "51.84 ", "192", "160004452752"],
  ["站塘|南坑小学", "25", "27.00 ", "100", "160004752371"],
  ["站塘|官村小学", "30", "24.30 ", "90", "490004451867"],
  ["站塘|幼儿园", "50", "42.66 ", "158", "160004401933"],
  ["站塘|华云希望小学", "35 ", "36.72 ", "136", "160004752307"],
  ["站塘|社山坝烤烟房", "30", "23.76", "88", "160004751593"],
  ["站塘|横岭村烤烟房", "35 ", "47.52 ", "176", "160004752485"],
  ["站塘|官山村烤烟房", "30 ", "25.92 ", "96", "160004752272"],
  ["站塘|卫生院", "45 ", "44.82 ", "166", "160004752484"],
  ["站塘|河头地面", "220 ", "249.75 ", "925", "160004450997"],
  ["文武坝|黄坊村委会", "30", "10.26 ", "38", ""],
  ["文武坝|联丰小学", "30", "34.56 ", "128", "150001288028"],
  ["文武坝|彭迳小学", "30", "143.10 ", "530", ""],
  ["文武坝|长塅小学", "50", "10.26 ", "38", "160004755018"],
  ["文武坝|白竹小学", "30", "32.40 ", "120", "160004752593"],
  ["文武坝|山新村保障房", "80", "55.08 ", "204", "150001280253"],
  ["文武坝|中山希望小学", "70", "75.60 ", "280", "150001281454"],
  ["文武坝|彭迳松山排安置区", "90", "10.26", "38", "150001280152"],
  ["文武坝|北寨村地面1", "1250 ", "934.74 ", "3462", "150001280007"],
  ["文武坝|北寨村地面2", "320 ", "653.00 ", "2420", "140000351823"],
  ["永隆|乡政府", "30", "26.46 ", "98", "160004401842"],
  ["永隆|卫生院", "30", "5.40 ", "20", "160004752486"],
  ["永隆|幼儿园", "30", "49.68 ", "184", "160004752306"],
  ["永隆|敬老院", "50", "31.05 ", "115", "160004752487"],
  ["永隆|中心小学", "80", "71.82 ", "260", "150001281888"],
  ["永隆|中学", "80", "73.98 ", "274", "150001281713"],
  ["永隆|果园场地面", "80", "79.92 ", "296", "160004451432"],
  ["永隆|永联村茶厂", "40", "35.1", "130", ""],
  ["富城|乡政府", "80", "76.14 ", "282", ""],
  ["富城|卫生院", "40", "41.04 ", "152", "150001288057"],
  ["富城|中心小学和幼儿园", "165", "134.19", "497", "150004336597"],
  ["富城|富城村村委会", "25", "22.68", "84", ""],
  ["富城|富城中学", "80", "74.52", "277", "150004336380"],
  ["富城|下山小组地面", "125", "125.28", "464", ""],
  ["富城|富东小学", "30", "24.80 ", "92", ""],
  ["富城|永隆果园场地面", "265", "261.36 ", "968", "160004811631"],
  ["麻州|麻州中学", "120 ", "259.74", "962", ""],
  ["麻州|麻州小学", "70", "103.14", "382", ""],
  ["麻州|幼儿园", "30", "48.6", "180", "150001280926"],
  ["麻州|湘江小学", "30", "33.48 ", "124", "160004751605"],
  ["麻州|东山小学", "50", "48.06", "178", "160004751606"],
  ["麻州|下堡小学和烤烟房", "70", "104.22 ", "386", "150001280751"],
  ["麻州|太坪脑小学", "30", "28.62", "106", ""],
  ["麻州|小河背小学", "30", "37.80 ", "140", "160004400285"],
  ["麻州|九州村卫生室和村委会", "30", "27 ", "100", "160004461008"],
  ["麻州|增丰村口保障房", "40.50 ", "150", "160004401841"],
  ["麻州|明德小学", "40 ", "8.10 ", "30", ""],
  ["麻州|增丰村地面", "450 ", "294.84 ", "1092", "150002508749"],
  ["麻州|增丰保障房后面地面", "390 ", "383.13 ", "1419", "150002508752"],
  ["珠兰|珠兰乡政府", "32.4", "32.4", "120", "160004451125"],
  ["珠兰|珠兰乡敬老院", "86.4", "86.4", "320", "160004451209"],
  ["珠兰|乡卫生院", "54", "54", "200", "160004450621"],
  ["珠兰|雁湖村", "27", "27", "100", ""],
  ["珠兰|上照村委", "21.6", "21.6", "80", "130002509615"],
  ["珠兰|杉坑", "37.8", "27", "100", ""],
  ["珠兰|上照村", "642.06", "652.86", "2418", ""],
  ["白鹅|计生办", "16.2", "16.2", "60", "160004401040"],
  ["白鹅|白鹅初中", "108", "108", "400", "160004451068"],
  ["白鹅|敬老院", "32.4", "32.4", "120", "160004401086"],
  ["白鹅|卫生院", "54", "54", "200", "160004451108"],
  ["白鹅|公办示范幼儿园", "35.1", "35.1", "130", ""],
  ["白鹅|中心小学", "83.7", "83.7", "310", ""],
  ["白鹅|狮子村小学", "43.2", "43.2", "160", "160004754278"],
  ["白鹅|洋口小学", "48.6", "48.6", "180", "129000675559"],
  ["白鹅|中心村烤烟房", "43.2", "43.2", "160", "160004451650"],
  ["白鹅|丹坑村小学", "27", "27", "100", "160004401038"],
  ["白鹅|梓坑村猪仔坳", "576.72", "576.72", "2136", ""],
  ["白鹅|中心村小学", "16", "16", "60", ""],
  ["西江|西江镇石门村", "2580", "2580", "9558", ""],
  ["庄埠|庄埠乡政府+派出所", "27", "27", "100", "160004754961"],
  ["庄埠|庄埠小学", "181.7", "181.71", "673", ""],
  ["庄埠|庄埠中学", "86.4", "86.4", "320", "150004337014"],
  ["庄埠|庄埠医院", "27", "27", "10", "160004754276"],
  ["庄埠|庄埠村烤烟房", "43.2", "43.2", "160", "160004451301"],
  ["庄口|庄口镇中学", "124.2", "124.2", "460", ""],
  ["庄口|庄口镇小学", "81", "81", "300", "150004337013"],
  ["庄口|汽车站", "59.4", "59.4", "220", "160004451176"],
  ["庄口|幼儿园", "43.2", "43.2", "160", "150001280800"],
  ["庄口|龙化小学", "54", "54", "200", "160004451220"],
  ["庄口|白沙村保障房", "16.2", "16.2", "60", "160004752445"],
  ["庄口|红岗社区", "27", "27", "100", ""],
  ["庄口|计生办", "21.6", "21.6", "80", "160004753145"],
  ["庄口|大陂保障房", "21.6", "21.6", "80", "160004751553"],
  ["庄口|华明希望小学", "21.6", "21.6", "80", "160004752446"],
  ["庄口|金汇希望小学", "27", "27", "100", "160004754277"],
  ["庄口|大排小学", "32.4", "32.4", "120", "160004751603"],
  ["庄口|庄口烟草房", "130.68", "130.68", "484", "150004336344"],
  ["庄口|禾坑小学", "32.4", "32.4", "120", "160004753147"],
  ["庄口|上芦小学", "37.8", "37.8", "140", ""],
  ["庄口|下芦小学", "37.8", "37.8", "140", ""],
  ["庄口|铺背小学", "16.2", "16.2", "60", "160004753146"],
  ["庄口|下坪小学", "37.8", "37.8", "140", ""],
  ["庄口|幼儿园后山地面", "259.74", "260.28", "964", "150001280347"],
  ["小密|小密乡政府", "32.4", "32.4", "120", "160004451047"],
  ["小密|计生所", "32.4", "32.4", "120", "130002509754"],
  ["小密|卫生院", "32.4", "32.4", "120", "130002509691"],
  ["小密|幼儿园", "32.4", "32.4", "120", "160004451048"],
  ["小密|小密小学", "260.28", "260.28", "964", "150001281520"],
  ["小密|小密中学", "86.4", "86.4", "320", "150001280349"],
  ["小密|半径村委会", "21.6", "21.6", "80", "160004451049"],
  ["小密|莲塘村委会", "23.76", "23.76", "88", "130002509728"],
  ["小密|莲塘小学", "27", "27", "100", "130002509696"],
  ["小密|半径小学", "21.6", "21.6", "80", "130002509612"],
  ["小密|烟草站", "331.56", "331.56", "1228", "160004451050"],
  ["晓龙|田尾小学", "32", "29.7", "110", "130001310368"],
  ["晓龙|高兰小学", "38", "35.64", "132", "130001310814"],
  ["晓龙|幼儿园+卫生院", "73", "83.7", "310", "160004452191"],
  ["晓龙|晓龙中学", "74", "78.84", "292", ""],
  ["晓龙|乡政府后山", "238", "236.79", "877", "160004451975"],
  ["晓龙|敬老院前坪", "65", "61.02", "226", "150001280274"],
  ["晓龙|晓龙村小", "200", "196.56", "728", ""],
  ["高排|高排中学", "95", "109.89", "407", "130002509611"],
  ["高排|明德小学", "71", "85", "315", "160004451712"],
  ["高排|云雷小学", "25", "25.92", "96", "130002509739"],
  ["高排|卫生院", "65", "54", "200", "130002509720"],
  ["高排|乡政府后山", "284", "265", "984", "160004451123"],
  ["右水|田丰村山地", "840", "840", "3111", "140000351471"],
  ["清溪|卫生院", "30", "30.24", "112", ""],
  ["清溪|敬老院", "66", "63.18", "234", ""],
  ["清溪|乡政府", "32", "29.7", "110", ""],
  ["清溪|小学", "55", "60.48", "224", ""],
  ["清溪|幼儿园", "54", "54", "200", ""],
  ["清溪|初中教学楼", "63", "63", "235", ""],
  ["筠门岭|黄埔小学", "43", "43.2", "160", "160004451789"],
  ["筠门岭|白埠小学", "46", "46.44", "172", ""],
  ["筠门岭|上增小学", "38", "37.8", "140", "130002509642"],
  ["筠门岭|镇政府", "158", "157.95", "585", "160004451580"],
  ["筠门岭|中心小学", "125", "124.74", "462", "160004452981"],
  ["筠门岭|小山初中", "150", "149.85", "555", ""],
  ["筠门岭|示范幼儿园", "30", "71.82", "266", "150001281634"],
  ["筠门岭|新增长岭小学", "70", "50.22", "186", "150001288041"],
  ["筠门岭|羊角小学1", "57", "68.58", "258", "160004451523"],
  ["筠门岭|敬老院", "50", "57.24", "212", "150004336346"],
  ["筠门岭|龙头小学", "80", "49.95", "185", ""],
  ["筠门岭|学子小学", "13", "13.5", "50", "130002509645"],
  ["筠门岭|石久下车坝", "480", "500", "1852", ""],
  ["筠门岭|大照地面", "390", "392.04", "1452", "160004451529"],
  ["筠门岭|大照烤烟房", "43", "160", "150001288005"],
  ["筠门岭|小照烤烟房", "130", "54", "200", "150001287982"],
  ["周田|法庭", "32", "32.4", "120", "160004400268"],
  ["周田|派出所", "32", "32.4", "120", "150004336481"],
  ["周田|周田中学", "589", "601", "2226", ""],
  ["周田|周田小学", "470", "327", "1211", ""],
  ["周田|秧排小学", "15", "16.2", "60", "160004402640"],
  ["周田|河墩小学", "36", "36", "132", "160004401430"],
  ["周田|司背小学", "36", "35.54", "132", "160004402638"],
  ["周田|紫云社区", "1280", "1410", "5222", "140000350819"],
  ["周田|小田烤烟房", "150", "150.12", "556", "150004336984"]
];
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

const MODBUS_DEVICE_MASTER_TYPES = [
  "03110B0D0001", "03110B0D0002"
];
var isModbusDeviceMaster = function (deviceType) {
  var found = _.findIndex(MODBUS_DEVICE_MASTER_TYPES, function (item) {
    return item === deviceType;
  });
  return -1 !== found;
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
  self.message(collectMessage, function (response) {
    if (response.retCode !== 200) {
      logger.debug(collectMessage);
      logger.error(response.retCode, response.description);
    }
    else {
      //如果是modbus采集器，那么不保存采集数据（采集的时候直接缓存到设备items）
      if (isModbusDeviceMaster(deviceInfo.type.id)) {
        return
      }
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
      //logger.debug(response.data);
    }
  });
};

var updateTool = function (self, devices) {
  _.forEach(devices, function (device) {
    var ret = _.findIndex(PLANT_ITEMS, function (plant) {
      return device.name === plant[0];
    });
    if (-1 === ret) {
      logger.debug("-------->" + device.name);
    }
    if (-1 !== ret) {
      logger.debug(PLANT_ITEMS[ret][0]);
      self.message({
        devices: self.configurator.getConfRandom("services.device_manager"),
        payload: {
          cmdName: "deviceUpdate",
          cmdCode: "0004",
          parameters: {
            "uuid": device.uuid,
            "extra.installedCapacity": parseInt(PLANT_ITEMS[ret][2]),
            "extra.PVCount": parseInt(PLANT_ITEMS[ret][3])
          }
        }
      });
      if ("" === PLANT_ITEMS[ret][4] || !PLANT_ITEMS[ret][4]) {
        return;
      }
      self.message({
        devices: self.configurator.getConfRandom("services.device_manager"),
        payload: {
          cmdName: "getDevice",
          cmdCode: "0003",
          parameters: {
            "userId": device.userId,
            "location.locationId": device.location.locationId,
            "type.id": "04110E0E0001"
          }
        }
      }, function (response) {
        if (200 === response.retCode) {
          var meters = response.data;
          logger.debug("=======>" + PLANT_ITEMS[ret][4]);
          _.forEach(meters, function (meter) {
            self.message({
              devices: self.configurator.getConfRandom("services.device_manager"),
              payload: {
                cmdName: "deviceUpdate",
                cmdCode: "0004",
                parameters: {
                  "uuid": meter.uuid,
                  "extra.slaveId": PLANT_ITEMS[ret][4]
                }
              }
            });
          })
        }
      })
    }
  });
};

function Collector(conx, uuid, token, configurator) {
  this.intervalId = null;
  this.collectMap = [];//[deviceType:"", cmd:{cmdName:"", cmdCode:"", parameters:[]}]
  this.collectPlant = function () {
    var self = this;
    var message = {
      devices: self.configurator.getConfRandom("services.device_manager"),
      payload: {
        cmdName: "getDevice",
        cmdCode: "0003",
        parameters: {
          "type.id": "010100000000"
        }
      }
    };
    self.message(message, function (response) {
        if (response.retCode !== 200) {
          return;
        }
        var devices = response.data;
        //updateTool(self, devices);//test
        var deviceGroups = _.groupBy(devices, "userId");
        _.forEach(deviceGroups, function (deviceGroup, userId) {
          var record = {
            "installedCapacity": 0,
            "PVCount": 0,
            "gatewayCount": 0,
            "inverterCount": 0,
            "inverterNormal": 0,
            "inverterAbnormal": 0,
            "inverterStandby": 0,
            "inverterEnergyTotal": 0,
            "inverterEnergyToday": 0,
            "PPvTotal": 0,
            "PAcTotal": 0,
            "FGrid": 0,
            "energyPositiveActive": 0,
            "energyReverseActive": 0
          };
          var sumRecord = _.reduce(deviceGroup, function (sum, n) {
            if (util.isNullOrUndefined(sum.extra) || util.isNullOrUndefined(sum.extra.items)) {
              logger.debug(sum);
              sum.extra.items = _.clone(record);
            }
            if (util.isNullOrUndefined(n.extra) || util.isNullOrUndefined(n.extra.items)) {
              logger.debug(n);
              n.extra.items = _.clone(record);
            }
            return {
              "extra": {
                "items": {
                  "installedCapacity": sum.extra.items["installedCapacity"] + n.extra.items["installedCapacity"],
                  "PVCount": sum.extra.items["PVCount"] + n.extra.items["PVCount"],
                  "gatewayCount": sum.extra.items["gatewayCount"] + n.extra.items["gatewayCount"],
                  "inverterCount": sum.extra.items["inverterCount"] + n.extra.items["inverterCount"],
                  "inverterNormal": sum.extra.items["inverterNormal"] + n.extra.items["inverterNormal"],
                  "inverterAbnormal": sum.extra.items["inverterAbnormal"] + n.extra.items["inverterAbnormal"],
                  "inverterStandby": sum.extra.items["inverterStandby"] + n.extra.items["inverterStandby"],
                  "inverterEnergyTotal": sum.extra.items["inverterEnergyTotal"] + n.extra.items["inverterEnergyTotal"],
                  "inverterEnergyToday": sum.extra.items["inverterEnergyToday"] + n.extra.items["inverterEnergyToday"],
                  "PPvTotal": sum.extra.items["PPvTotal"] + n.extra.items["PPvTotal"],
                  "PAcTotal": sum.extra.items["PAcTotal"] + n.extra.items["PAcTotal"],
                  "FGrid": sum.extra.items["FGrid"] + n.extra.items["FGrid"],
                  "energyPositiveActive": sum.extra.items["energyPositiveActive"] + n.extra.items["energyPositiveActive"],
                  "energyReverseActive": sum.extra.items["energyReverseActive"] + n.extra.items["energyReverseActive"]
                }
              }
            }
          });
          sumRecord.extra.items.FGrid = sumRecord.extra.items.FGrid / deviceGroup.length;
          self.message({
            devices: self.configurator.getConfRandom("services.device_manager"),
            payload: {
              cmdName: "deviceUpdate",
              cmdCode: "0004",
              parameters: {
                "uuid": userId,
                "extra.items": sumRecord.extra.items,
                "timeZone": {
                  "id": "Asia/Shanghai",
                  "offset": 28800000
                }
              }
            }
          });
          var dataRecords = [];
          _.forEach(sumRecord.extra.items, function (value, key) {
            dataRecords.push({"name": key, "value": value});
          });
          var putDataMsg = {
            devices: self.configurator.getConfRandom("services.data_manager"),
            payload: {
              cmdName: "putData",
              cmdCode: "0004",
              parameters: {
                uuid: userId,
                type: "060A08000000",
                timestamp: new Date().toISOString(),
                offset: 28800000,
                data: dataRecords
              }
            }
          };
          self.message(putDataMsg, function (response) {
            if (response.retCode !== 200) {
              logger.error(response.retCode, response.description);
            }
          });
        });
      }
    );
  }
  ;
  this.collectThermostat = function () {
    var self = this;
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
            var temperature = parseFloat(deviceInfo.extra.items.dis_temp.replace(/c/g, ""));
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
  this.getCollectConf = function (deviceType) {
    var self = this;
    for (var i = 0, len = self.collectMap.length; i < len; ++i) {
      if (self.collectMap[i].deviceType === deviceType) {
        return self.collectMap[i];
      }
    }
  };
  this.collect = function (self) {
    _.forEach(self.collectMap, function (mapItem) {
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
          return;
        }
        var devices = response.data;
        _.forEach(devices, function (deviceInfo) {
          if (true === deviceInfo.online && deviceInfo.status && "CONNECTED" === deviceInfo.status.network) {
            collect(self, deviceInfo);//设备离线，则不采集
          }
        });
      })
    });
    /*
    *单独采集月动温控器
    * */
    self.collectThermostat();
    self.collectPlant();
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