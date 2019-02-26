const MongoDB = require('./lib/mongoDB/mongoDB');
const LevelDB = require('./lib/levelDB/levelDB');

// Singleton support
let instance = null;

/**
 * Private function to checks and establish a connection
 * if there isn't one already.
 * @param  {Object} database [description]
 * @return {Promise}          [description]
 */
function checkConnectionAsync(database) {
  return new Promise((resolve, reject) => {
    if (database.isConnected) {
      resolve();
    } else {
      database.connectAsync().then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    }
  });
}

class MasterRepository {
  /**
   * Constructor
   * @param {Boolean} isBackup [description]
   * @param {Object}  config   [description]
   */
  constructor(isBackup, config) {
    if (isBackup) {
      this.database = MongoDB.createInstance(config);
    } else {
      this.database = LevelDB.createInstance(config);
    }
  }

  /**
   * Fetches the current blockchain in its entirety
   * @return {Promise} [description]
   */
  getBlockchainAsync() {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.blockchainRepository.getBlockchainAsync();
      }).then((blocks) => {
        resolve(blocks);
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Fetches a single block from the blockchain.
   * The block that will be fetched is determined by
   * the given {blockNumber}.
   * @param  {Integer} blockNumber [description]
   * @return {Promise}             [description]
   */
  getBlockAsync(blockNumber) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.blockchainRepository.getBlockAsync(blockNumber);
      }).then((block) => {
        resolve(block);
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Persists one or multiple blocks into the blockchain
   * @param  {Block[]} block [description]
   * @return {Promise}       [description]
   */
  putBlocksAsync(block) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.blockchainRepository.putBlocksAsync(block);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Delete one or multiple blocks.
   * The blocks that will be deleted are determined by
   * the given {blockNumbers}.
   * @param  {Integer[]} blockNumbers [description]
   * @return {Promise}              [description]
   */
  deleteBlocksAsync(blockNumbers) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.blockchainRepository.deleteBlocksAsync(blockNumbers);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Fetches the current global state in its entirety
   * @return {Promise} [description]
   */
  getGlobalStateAsync() {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.globalStateRepository.getGlobalStateAsync();
      }).then((states) => {
        resolve(states);
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Fetches a single state from the global state.
   * The state that will be fetched is determined by
   * the given {publicKey}.
   * @param  {String} publicKey [description]
   * @return {Promise}           [description]
   */
  getStateAsync(publicKey) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.globalStateRepository.getStateAsync(publicKey);
      }).then((state) => {
        resolve(state);
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Persists one or multiple states into the global state
   * @param  {State[]} states [description]
   * @return {Promise}        [description]
   */
  putStatesAsync(states) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.globalStateRepository.putStatesAsync(states);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * Updates the global state.
   * The updates are determined by the given {transactions}.
   * Each transaction will generate two states: one for receiver and one for sender.
   * The generated states will merge with the current states in the global state.
   * @param  {Transaction[]} transactions [description]
   * @return {Promise}              [description]
   */
  updateGlobalStateAsync(transactions) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.globalStateRepository.updateGlobalStateAsync(transactions);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * [clearGlobalStateAsync description]
   * @param  {Transaction[]} transactions [description]
   * @return {Promise}              [description]
   */
  clearGlobalStateAsync(transactions) {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        return this.database.globalStateRepository.clearGlobalStateAsync(transactions);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  /**
   * [connectAsync description]
   * @return {Promise} [description]
   */
  connectAsync() {
    return new Promise((resolve, reject) => {
      checkConnectionAsync(this.database).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }
}

module.exports = {
  /**
   * Creates a new instance of a MasterRepository object
   * @param  {Boolean} isBackup [description]
   * @param  {Object}  config   [description]
   * @return {MasterRepository}           [description]
   */
  getInstance(isBackup, config) {
    if (!instance) {
      instance = new MasterRepository(isBackup, config);
    }
    return instance;
  },
};
