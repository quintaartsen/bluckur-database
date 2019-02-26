const log = require('debug')('bluckur-database');
const error = require('debug')('bluckur-database:error');

class BlockchainMongoRepository {
  constructor(BlockModel) {
    this.BlockModel = BlockModel;
  }

  getBlockchainAsync() {
    return new Promise((resolve, reject) => {
      log('fetching blockchain ...');
      this.BlockModel.find({}, { _id: 0, __v: 0 }, (err, blocks) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log(`${blocks.length} block(s) fetched`);
          resolve(blocks);
        }
      });
    });
  }

  getBlockAsync(blockNumber) {
    return new Promise((resolve, reject) => {
      log(`fetching block ... [blockNumber=${blockNumber}]`);
      this.BlockModel.findOne({ 'blockHeader.blockNumber': blockNumber }, { _id: 0, __v: 0 }, (err, block) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          if (block) {
            log(`block fetched [blockHash=${block.blockHeader.blockHash}]`);
          } else {
            error(`Key not found in database [${blockNumber}]`);
          }
          resolve(block);
        }
      });
    });
  }

  putBlocksAsync(blocks) {
    return new Promise((resolve, reject) => {
      log('putting blocks ...');
      this.BlockModel.findOneAndUpdate();
      this.BlockModel.insertMany(blocks, (err) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log(`${blocks.length} block(s) put`);
          resolve();
        }
      });
    });
  }

  deleteBlocksAsync(blockNumbers) {
    return new Promise((resolve, reject) => {
      log(`deleting blocks ... [blockNumbers=${blockNumbers}]`);
      this.BlockModel.deleteMany({ 'blockHeader.blockNumber': { $in: blockNumbers } }, (err) => {
        if (err) {
          error(err.message);
          reject(err);
        } else {
          log(`${blockNumbers.length} block(s) deleted`);
          resolve();
        }
      });
    });
  }
}

module.exports = {
  createInstance(BlockModel) {
    if (!BlockModel) {
      throw new Error('Invalid argument(s)');
    }
    return new BlockchainMongoRepository(BlockModel);
  },
};
