"use strict";

// Colyseus 0.14 removes the second close listener by index during LEAVE_ROOM.
// Observe dispatch without adding/reordering listeners in that legacy array.
module.exports = function observeSocketClose(socket, onClose) {
  const emit = socket.emit;
  socket.emit = function (eventName, ...args) {
    if (eventName === "close") onClose(...args);
    return emit.call(this, eventName, ...args);
  };
};
