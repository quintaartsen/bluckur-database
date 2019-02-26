const mongoose = require('mongoose');
const log = require('debug')('bluckur-database');
const Models = require('bluckur-models');
const defaultConfig = require('./defaultConfig');
const BlockchainMongoRepository = require('./repositories/blockchainMongoRepository');
const GlobalStateMongoRepository = require('./repositories/globalStateMongoRepository');

class MongoDB {
  constructor(customConfig) {
    this.config = Object.assign(defaultConfig, customConfig);
    this.BlockModel = mongoose.model('block', mongoose.Schema(Models.blockBlueprint));
    this.StateModel = mongoose.model('state', mongoose.Schema(Models.stateBlueprint));
    this.blockchainRepository = null;
    this.globalStateRepository = null;
    this.isConnected = false;
  }

  connectAsync() {
    return new Promise((resolve, reject) => {
      const {
        host, port, db, user, password,
      } = this.config;
      mongoose.connect(`mongodb://${user}:${password}@${host}:${port}/${db}`).then(() => {
        this.blockchainRepository = BlockchainMongoRepository.createInstance(this.BlockModel);
        this.globalStateRepository = GlobalStateMongoRepository.createInstance(this.StateModel);
        this.isConnected = true;
        log(`connected with mongoDB [${host}:${port}]`);
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }
}

module.exports = {
  createInstance(customConfig) {
    return new MongoDB(customConfig);
  },
};
