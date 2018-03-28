var globals = {
  redisClient: null,
  elastic: null,
  amqpConn: null,
  pubChannel: null,
  consumeChannel: null,
  systemCollection: null,
  startFunction: null,
  server: null,
  multi: {},
  destinations: []
}

module.exports = globals
