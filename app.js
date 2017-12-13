var mongoose = require('mongoose')
var amqp = require('amqplib/callback_api')
var waterfall = require('async-waterfall')
var request = require('then-request')
const uuidv4 = require('uuid/v1')
var amqpConn = null
var consumeChannel = null
var pubChannel = null
var systemCollection = null
var objectsToTransmit = null

function consumeMessage (message, callback1, callback2) {
  var messageString = message.content.toString()
  var foundSomething = false
  logDirectorStart(message.properties.correlationId)
  objectsToTransmit.forEach(function (item, i, arr) {
    if (item.objectId === message.fields.routingKey) {
      foundSomething = true
      if ('preloadActions' in item) {
        waterfall([function initializer (firstMapFunction) {
          var initialValue = messageString
          firstMapFunction(null, initialValue)
        }].concat(item.preloadActions.map(function (arrayItem) {
          return function (lastItemResult, nextCallback) {
            var paramString = ''
            arrayItem.actionParameters.forEach(function (item, i, arr) {
              paramString = paramString + 'param' + (i + 1) + '=' + item + '&'
            })

            request('POST',
              process.env.ACTION_SCRIPTER_URL + arrayItem.actionId + '?' + paramString,
              {body: lastItemResult}
            ).getBody('utf8')
            .done(function (res) {
              nextCallback(null, res)
            })
          }
        })), function (err, result) {
          if (err) {
            console.error(err.message)
          }
          console.log('[RESULT AFTER ALL ACTIONS]')
          message.content = Buffer.from(result)
          callback1(message, item, callback2)
        })
      } else {
        callback1(message, item, callback2)
      }
    }
  })
  if (!foundSomething) {
    console.log('[ACKING]')
    consumeChannel.ack(message)
  }
}

function getObjectsToTransmit () {
  systemCollection.findOne({ systemId: process.env.SYSTEM_ID }, function (err, result) {
    if (err) {
      console.error(err.message)
    }
    if (result) {
      objectsToTransmit = result.objectTypes
    } else {
      console.error('[mongo]', 'Wrong system id')
      process.exit(1)
    }
  })
}

function publishToChannel (queueName, content, correlationId) {
  const newMessageUid = uuidv4()
  console.log('[PUBLISHING] ' + queueName)
  pubChannel.assertQueue(queueName)
  pubChannel.sendToQueue(queueName, content, { correlationId: correlationId, messageId: newMessageUid },
                    function (err, ok) {
                      if (err) {
                        console.error('[AMQP publish] publish', err)
                        pubChannel.connection.close()
                      }

                      logDestinationAdded(correlationId, queueName.replace('.outgoing', ''), newMessageUid)
                    })
}

function publish (content, objectType, callback) {
  try {
    if (!pubChannel) {
      amqpConn.createConfirmChannel(function (err, ch) {
        if (err) {
          console.error('[AMQP publish]', err.message)
          return setTimeout(start, 1000)
        }
        ch.on('error', function (err) {
          console.error('[AMQP publish] channel error', err.message)
        })
        ch.on('close', function () {
          console.log('[AMQP publish] channel closed')
          pubChannel = null
        })

        pubChannel = ch
        objectType.destinations.forEach(function (objectItem, i, arr) {
          request('POST',
            process.env.SPLITTER_URL + objectItem.split,
            {body: content.content.toString()}
          ).getBody('utf8')
          .done(function (res) {
            if (res[0] === '[') {
              const messageArray = JSON.parse(res)
              messageArray.forEach(function (item, i, arr) {
                publishToChannel(objectItem.systemId + '.outgoing', Buffer.from(JSON.stringify(item)), content.properties.correlationId)
              })
            } else {
              publishToChannel(objectItem.systemId + '.outgoing', content.content, content.properties.correlationId)
            }
          })
        })
      })
    } else {
      objectType.destinations.forEach(function (objectItem, i, arr) {
        request('POST',
          process.env.SPLITTER_URL + objectItem.split,
          {body: content.content.toString()}
        ).getBody('utf8')
        .done(function (res) {
          if (res[0] === '[') {
            const messageArray = JSON.parse(res)
            messageArray.forEach(function (item, i, arr) {
              publishToChannel(objectItem.systemId + '.outgoing', Buffer.from(JSON.stringify(item)), content.properties.correlationId)
            })
          } else {
            publishToChannel(objectItem.systemId + '.outgoing', content.content, content.properties.correlationId)
          }
        })
      })
    }
  } catch (e) {
    console.error('[AMQP publish]', e.message)
  }
  console.log('[ACKING]')
  consumeChannel.ack(content)
}

function consumer () {
  try {
    if (!consumeChannel) {
      amqpConn.createConfirmChannel(function (err, ch) {
        if (err) {
          console.error('[AMQP consume]', err.message)
          return setTimeout(start, 1000)
        }
        ch.on('error', function (err) {
          console.error('[AMQP consume] channel error', err.message)
        })
        ch.on('close', function () {
          console.log('[AMQP consume] channel closed')
          consumeChannel = null
        })

        ch.prefetch(100)

        consumeChannel = ch
        consumeChannel.assertQueue(process.env.SYSTEM_ID)
        consumeChannel.consume(process.env.SYSTEM_ID, function (content) {
          if (content !== null) {
            console.log('[NEW MESSAGE TO CONSUME]')
            consumeMessage(content, publish, consumeChannel.ack)
          }
        })
      })
    } else {
      consumeChannel.consume(process.env.SYSTEM_ID, function (content) {
        if (content !== null) {
          console.log('[NEW MESSAGE TO CONSUME]')
          consumeMessage(content, publish, consumeChannel.ack)
        }
      })
    }
  } catch (e) {
    console.error('[AMQP consume]', e.message)
  }
}

function start () {
  amqp.connect(process.env.AMQP_URL + '?heartbeat=60', function (err, conn) {
    if (err) {
      console.error('[AMQP connect]', err.message)
      return setTimeout(start, 1000)
    }
    conn.on('error', function (err) {
      if (err.message !== 'Connection closing') {
        console.error('[AMQP connect] conn error', err.message)
      }
    })
    conn.on('close', function () {
      console.error('[AMQP connect] reconnecting')
      return setTimeout(start, 1000)
    })
    console.log('[AMQP connect] connected')
    amqpConn = conn
    whenConnected()
  })
}

function whenConnected () {
  mongoose.connect(process.env.MONGO_URL, function (err) {
    if (err) {
      console.error('[mongo]', err.message)
    }

    systemCollection = mongoose.connection.db.collection('systems')
    getObjectsToTransmit()
  })

  consumer()
}

start()

function logDestinationAdded (correlationId, systemId, newMessageUid) {
  request('GET', process.env.LOGGER_URL + 'destinationAdded/' + correlationId + '/' + systemId + '/' + newMessageUid + '/' + Date.now())
}

function logDirectorStart (correlationId) {
  request('GET', process.env.LOGGER_URL + 'update/' + correlationId + '/timeDirectorStart/' + Date.now())
}
