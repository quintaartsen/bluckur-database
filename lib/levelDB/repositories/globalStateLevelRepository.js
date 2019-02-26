const log = require('debug')('bluckur-database');
const error = require('debug')('bluckur-database:error');
const TransactionConverter = require('./../../util/transactionConverter');

class GlobalStateLevelRepository {
  constructor(globalStateLevel) {
    this.globalStateLevel = globalStateLevel;
  }

  getGlobalStateAsync() {
    return new Promise((resolve, reject) => {
      const states = [];
      log('fetching global state ...');
      this.globalStateLevel.createValueStream()
        .on('data', (value) => {
          states.push(JSON.parse(value));
        })
        .on('error', (err) => {
          error(err.message);
          reject(err);
        })
        .on('end', () => {
          log(`${states.length} state(s) fetched`);
          resolve(states);
        });
    });
  }

  getStateAsync(publicKey) {
    return new Promise((resolve, reject) => {
      log(`fetching state ... [publicKey=${publicKey}]`);
      this.globalStateLevel.get(publicKey).then((value) => {
        const state = JSON.parse(value);
        log(`state fetched [coin=${state.coin}, stake=${state.stake}]`);
        resolve(state);
      }).catch((err) => {
        error(err.message);
        reject(err);
      });
    });
  }

  putStatesAsync(states) {
    return new Promise((resolve, reject) => {
      if (states) {
        const ops = states.map((state) => {
          return {
            type: 'put',
            key: state.publicKey,
            value: JSON.stringify(state),
          };
        });
        log('putting states ...');
        this.globalStateLevel.batch(ops).then(() => {
          log(`${states.length} state(s) put`);
          resolve();
        }).catch((err) => {
          error(err.message);
          reject(err);
        });
      } else {
        resolve();
      }
    });
  }

  updateGlobalStateAsync(transactions) {
    return new Promise((resolve, reject) => {
      const pendingStates = TransactionConverter.convertToStates(transactions);
      log('updating global state ...');
      const promises = [];
      pendingStates.forEach((pendingState) => {
        promises.push(this.convertToUpdatedStateAsync(pendingState));
      });
      Promise.all(promises).then((states) => { return this.putStatesAsync(states); }).then(() => {
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
      this.getGlobalStateAsync().then((states) => {
        const ops = states.map((state) => {
          return {
            type: 'del',
            key: state.publicKey,
          };
        });
        log('putting states ...');
        return this.globalStateLevel.batch(ops);
      }).then(() => {
        log('global state cleared');
        resolve();
      }).catch((err) => {
        log(err.message);
        reject(err);
      });
    });
  }

  convertToUpdatedStateAsync(pendingState) {
    return new Promise((resolve, reject) => {
      const { publicKey, coin, stake } = pendingState;
      this.getStateAsync(publicKey).then((state) => {
        resolve({
          publicKey,
          coin: state.coin + coin,
          stake: state.stake + stake,
        });
      }).catch((err) => {
        if (err.notFound) {
          resolve({
            publicKey,
            coin,
            stake,
          });
        } else {
          reject(err);
        }
      });
    });
  }
}

module.exports = {
  /**
   * Creates a instance of a GlobalStateLevelRepository object
   * @param  {LevelUP} globalStateLevel [description]
   * @return {GlobalStateLevelRepository}                  [description]
   */
  createInstance(globalStateLevel) {
    if (!globalStateLevel) {
      throw new Error('Invalid argument(s)');
    }
    return new GlobalStateLevelRepository(globalStateLevel);
  },
};
