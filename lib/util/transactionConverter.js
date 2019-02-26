const Models = require('bluckur-models');

function addPendingStateToStates(states, publicKey, type, amount, isRecipient) {
  const multiplier = isRecipient ? 1 : -1;
  const state = states.find((s) => { return s.publicKey === publicKey; });
  if (state) {
    state[type] += amount * multiplier;
  } else {
    states.push(Models.createStateInstance({
      publicKey,
      coin: type === 'coin' ? amount * multiplier : 0,
      stake: type === 'stake' ? amount * (multiplier === -1 ? 0 : 1) : 0,
    }));
  }
}

module.exports = {
  convertToStates(transactions) {
    const states = [];
    transactions.forEach(({
      recipient, sender, type, amount,
    }) => {
      addPendingStateToStates(states, recipient, type, amount, true);
      addPendingStateToStates(states, sender, type, amount);
    });
    return states;
  },
};
