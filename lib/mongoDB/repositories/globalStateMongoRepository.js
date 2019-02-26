const log = require('debug')('bluckur-database');
const error = require('debug')('bluckur-database:error');
const TransactionConverter = require('./../../util/transactionConverter');

class GlobalStateMongoRepository {
  constructor(StateModel) {
    this.StateModel = StateModel;
  }

  getGlobalStateAsync() {
    return new Promise((resolve, reject) => {
      log('fetching global state ...');
      this.StateModel.find({}, { _id: 0, __v: 0 }, (err, states) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log(`${states.length} state(s) fetched`);
          resolve(states);
        }
      });
    });
  }

  getStateAsync(publicKey) {
    return new Promise((resolve, reject) => {
      log(`fetching state ... [publicKey=${publicKey}]`);
      this.StateModel.findOne({ publicKey }, { _id: 0, __v: 0 }, (err, state) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          if (state) {
            log(`state fetched [coin=${state.coin}, stake=${state.stake}]`);
          } else {
            error(`Key not found in database [${publicKey}]`);
          }
          resolve(state);
        }
      });
    });
  }

  putStatesAsync(states) {
    return new Promise((resolve, reject) => {
      log('putting states ...');
      this.StateModel.insertMany(states, (err) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log(`${states.length} state(s) put`);
          resolve();
        }
      });
    });
  }

  updateGlobalStateAsync(transactions) {
    return new Promise((resolve, reject) => {
      const pendingStates = TransactionConverter.convertToStates(transactions);
      log('updating global state ...');
      const promises = [];
      pendingStates.forEach((pendingState) => {
        promises.push(this.findAndUpdateAsync(pendingState));
      });
      Promise.all(promises).then(() => {
        log(`${pendingStates.length} state(s) updated`);
        resolve();
      }).catch((err) => {
        error(err.message);
        reject(err);
      });
    });
  }

  clearGlobalStateAsync() {
    return new Promise((resolve, reject) => {
      log('clearing global state ...');
      this.StateModel.deleteMany({}, (err) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log('global state cleared');
          resolve();
        }
      });
    });
  }

  findAndUpdateAsync({ publicKey, coin, stake }) {
    return new Promise((resolve, reject) => {
      this.getStateAsync(publicKey).then((state) => {
        const updatedState = {
          publicKey,
          coin: (state ? state.coin : 0) + coin,
          stake: (state ? state.stake : 0) + stake,
        };
        return state ? this.updateOneStateAsync(publicKey, updatedState) :
          this.putStatesAsync([updatedState]);
      }).then(() => {
        resolve();
      }).catch((err) => {
        reject(err);
      });
    });
  }

  updateOneStateAsync(publicKey, updatedState) {
    return new Promise((resolve, reject) => {
      this.StateModel.updateOne({ publicKey }, updatedState, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }
}

module.exports = {
  createInstance(StateModel) {
    if (!StateModel) {
      throw new Error('Invalid argument(s)');
    }
    return new GlobalStateMongoRepository(StateModel);
  },
};
