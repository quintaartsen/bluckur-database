const level = require('level');
const log = require('debug')('bluckur-database');
const defaultConfig = require('./defaultConfig');
const BlockchainLevelRepository = require('./repositories/blockchainLevelRepository');
const GlobalStateLevelRepository = require('./repositories/globalStateLevelRepository');

class LevelDB {
  constructor(customConfig) {
    this.config = Object.assign(defaultConfig, customConfig);
    this.blockchainRepository = null;
    this.globalStateRepository = null;
    this.isConnected = false;
  }

  connectAsync() {
    return new Promise((resolve, reject) => {
      const { blockchainPath, globalStatePath } = this.config;
      Promise.all([this.openBlockchainLevelAsync(), this.openGlobalStateLevelAsync()]).then(() => {
        this.isConnected = true;
        log(`connected with levelDB [${blockchainPath}, ${globalStatePath}]`);
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  openBlockchainLevelAsync() {
    return new Promise((resolve, reject) => {
      level(this.config.blockchainPath, this.config.options, (err, db) => {
        if (err) {
          reject(err);
        } else {
          this.blockchainRepository = BlockchainLevelRepository.createInstance(db);
          resolve();
        }
      });
    });
  }

  openGlobalStateLevelAsync() {
    return new Promise((resolve, reject) => {
      level(this.config.globalStatePath, this.config.options, (err, db) => {
        if (err) {
          reject(err);
        } else {
          this.globalStateRepository = GlobalStateLevelRepository.createInstance(db);
          resolve();
        }
      });
    });
  }
}

module.exports = {
  createInstance(customConfig) {
    return new LevelDB(customConfig);
  },
};
