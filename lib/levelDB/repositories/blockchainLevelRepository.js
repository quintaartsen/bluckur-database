const log = require('debug')('bluckur-database');
const error = require('debug')('bluckur-database:error');

class BlockchainLevelRepository {
  constructor(blockchainLevel) {
    this.blockchainLevel = blockchainLevel;
  }

  getBlockchainAsync() {
    return new Promise((resolve, reject) => {
      const blocks = [];
      log('fetching blockchain ...');
      this.blockchainLevel.createValueStream()
        .on('data', (value) => {
          blocks.push(JSON.parse(value));
        })
        .on('error', (err) => {
          error(err.message);
          reject(err);
        })
        .on('end', () => {
          log(`${blocks.length} block(s) fetched`);
          resolve(blocks);
        });
    });
  }

  getBlockAsync(blockNumber) {
    return new Promise((resolve, reject) => {
      log(`fetching block ... [blockNumber=${blockNumber}]`);
      this.blockchainLevel.get(blockNumber).then((value) => {
        const block = JSON.parse(value);
        log(`block fetched [blockHash=${block.blockHeader.blockHash}]`);
        resolve(block);
      }).catch((err) => {
        error(err.message);
        reject(err);
      });
    });
  }

  putBlocksAsync(blocks) {
    return new Promise((resolve, reject) => {
      const ops = blocks.map((block) => {
        return {
          type: 'put',
          key: block.blockHeader.blockNumber,
          value: JSON.stringify(block),
        };
      });
      log('putting blocks ...');
      this.blockchainLevel.batch(ops).then(() => {
        log(`${blocks.length} block(s) put`);
        resolve();
      }).catch((err) => {
        error(err.message);
        reject(err);
      });
    });
  }

  deleteBlocksAsync(blockNumbers) {
    return new Promise((resolve, reject) => {
      const ops = blockNumbers.map((blockNumber) => {
        return {
          type: 'del',
          key: blockNumber,
        };
      });
      log(`deleting blocks ... [blockNumbers=${blockNumbers}]`);
      this.blockchainLevel.batch(ops).then(() => {
        log(`${blockNumbers.length} block(s) deleted`);
        resolve();
      }).catch((err) => {
        error(err.message);
        reject(err);
      });
    });
  }
}

module.exports = {
  createInstance(blockchainLevel) {
    if (!blockchainLevel) {
      throw new Error('Invalid argument(s)');
    }
    return new BlockchainLevelRepository(blockchainLevel);
  },
};
