var globals = require('./include/globals.js')
var jsonpath = require('jsonpath')
var mongoose = require('mongoose')
var redis = require('redis')
var amqp = require('amqplib/callback_api')
var waterfall = require('async-waterfall')
var request = require('then-request')
var elasticsearch = require('elasticsearch')
const uuidv5 = require('uuid/v5')
const uuidv4 = require('uuid/v4')
const uuidv1 = require('uuid/v1')
var objectsToTransmit = null
var userId = null

function getObjectsToTransmit () {
  globals.systemCollection.findOne({ systemId: process.env.SYSTEM_ID }, function (err, result) {
    if (err) {
      console.error(err.message)
    }
    if (result) {
      objectsToTransmit = result.objectTypes
      userId = result.userId
    } else {
      console.error('[mongo]', 'Wrong system id')
      return setTimeout(start, 1000)
    }
  })
}

function consumeMessage (originalMessage) {
  var messageString = originalMessage.content.toString()
  var foundSomething = false
  logDirectorStart(originalMessage.properties.correlationId)

  objectsToTransmit.forEach(function (objectType, i, arr) {
    if (objectType.objectId === originalMessage.fields.routingKey) {
      foundSomething = true
      // send to elasticsearch to analyze
      if ('analyzer' in objectType) {
        var messageObject = JSON.parse(messageString)
        var analyticsId = originalMessage.properties.correlationId
        var analyticsType = objectType.objectId
        var analyticsObject = {
          timestamp: Date.now(),
          filters: {},
          values: {}
        }

        if ('typeField' in objectType.analyzer) {
          analyticsType = messageObject[objectType.analyzer.typeField]
        }

        if ('idField' in objectType.analyzer) {
          analyticsId = messageObject[objectType.analyzer.idField]
        }

        if ('timeField' in objectType.analyzer) {
          analyticsObject.timestamp = messageObject[objectType.analyzer.timeField]
        }

        if ('filters' in objectType.analyzer) {
          objectType.analyzer.filters.forEach(function (filterField, i, arr) {
            analyticsObject.filters[filterField] = messageObject[filterField]
          })
        }

        if ('values' in objectType.analyzer) {
          objectType.analyzer.values.forEach(function (valueField, i, arr) {
            analyticsObject.values[valueField] = messageObject[valueField]
          })
        }

        globals.elastic.index({
          index: userId,
          type: analyticsType,
          id: analyticsId,
          body: analyticsObject
        }, function (error, response) {
          if (error) {
            console.log('[ELASTIC ERROR] ' + error)
          }
        })
      }

      globals.multi[originalMessage.properties.correlationId] = globals.redisClient.multi()
      if ('preloadActions' in objectType) {
        waterfall([function initializer (firstMapFunction) {
          var initialValue = messageString
          firstMapFunction(null, initialValue)
        }].concat(objectType.preloadActions.map(function (arrayItem) {
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
          publish(originalMessage, result, objectType)
        })
      } else {
        publish(originalMessage, originalMessage.content.toString(), objectType)
      }
    }
  })
  if (!foundSomething) {
    console.log('[ACKING]')
    globals.consumeChannel.ack(originalMessage)
  }
}

function publish (originalMessage, messageAfterActions, objectType) {
  var destinationsTotal = objectType.destinations.length
  var destinationsDone = 0
  globals.destinations = []
  objectType.destinations.forEach(function (objectItem, i, arr) {
    var shouldBeRouted = true
    if ('routeCondition' in objectItem && objectItem.routeCondition !== '') {
      var elements = jsonpath.query(JSON.parse(originalMessage.content.toString()), objectItem.routeCondition)
      shouldBeRouted = (elements.length > 0)
      console.log('[CHECKING ROUTE CONDITION] ' + shouldBeRouted)
    } else {
      console.log('[NO CHECKING ROUTE CONDITION]')
    }
    if (shouldBeRouted) {
      request('POST',
        process.env.SPLITTER_URL + objectItem.split,
        {body: messageAfterActions}
      ).getBody('utf8')
      .done(function (messageAfterSplitter) {
        destinationsDone++
        var lastDestination = false
        if (destinationsDone === destinationsTotal) {
          lastDestination = true
        }
        if (messageAfterSplitter[0] === '[') {
          const messageArray = JSON.parse(messageAfterSplitter)
          messageArray.forEach(function (item, i, arr) {
            var lastSplitted = false
            if (lastDestination && i === (arr.length - 1)) {
              lastSplitted = true
            }
            publishToChannel(objectItem.systemId + '.outgoing', JSON.stringify(item), originalMessage, lastSplitted)
          })
        } else {
          publishToChannel(objectItem.systemId + '.outgoing', messageAfterSplitter, originalMessage, lastDestination)
        }
      })
    } else {
      destinationsDone++
      if (destinationsDone === destinationsTotal) {
        globals.multi[originalMessage.properties.correlationId].exec(function (err, replies) {
          if (err) {
            console.error('[REDIS publish] error ', err)
            globals.multi[originalMessage.properties.correlationId].discard()
            globals.multi[originalMessage.properties.correlationId] = null
          } else {
            console.log('[ACKING] ' + originalMessage.properties.correlationId)
            globals.consumeChannel.ack(originalMessage)
            globals.multi[originalMessage.properties.correlationId] = null
            globals.destinations.forEach(function (objectItem, i, arr) {
              logDestinationAdded(originalMessage.properties.correlationId, objectItem.name, objectItem.messageUid)
            })
          }
        })
      }
    }
  })
}

function publishToChannel (queueName, messageAfterActions, originalMessage, lastDestination) {
  const newMessageUid = uuidv5(uuidv4(), uuidv1())
  globals.multi[originalMessage.properties.correlationId]
    .hmset(newMessageUid,
       'message', messageAfterActions,
       'correlationId', originalMessage.properties.correlationId,
       'queue', queueName,
       'publishTime', originalMessage.properties.timestamp)
    .zadd(queueName, originalMessage.properties.timestamp, newMessageUid)
  globals.destinations.push({ name: queueName.replace('.outgoing', ''), messageUid: newMessageUid })
  if (lastDestination) {
    globals.multi[originalMessage.properties.correlationId].exec(function (err, replies) {
      if (err) {
        console.error('[REDIS publish] error ', err)
        globals.multi[originalMessage.properties.correlationId].discard()
        globals.multi[originalMessage.properties.correlationId] = null
      } else {
        console.log('[ACKING] ' + originalMessage.properties.correlationId)
        globals.consumeChannel.ack(originalMessage)
        globals.multi[originalMessage.properties.correlationId] = null
        globals.destinations.forEach(function (objectItem, i, arr) {
          logDestinationAdded(originalMessage.properties.correlationId, objectItem.name, objectItem.messageUid)
        })
      }
    })
  }
}

function consumer () {
  try {
    if (!globals.consumeChannel) {
      globals.amqpConn.createConfirmChannel(function (err, ch) {
        if (err) {
          console.error('[AMQP consume]', err.message)
          return setTimeout(start, 1000)
        }
        ch.on('error', function (err) {
          console.error('[AMQP consume] channel error', err.message)
          process.exit(1)
        })
        ch.on('close', function () {
          console.log('[AMQP consume] channel closed')
          process.exit(1)
        })

        ch.prefetch(20)

        globals.consumeChannel = ch
        globals.consumeChannel.assertQueue(process.env.SYSTEM_ID)
        globals.consumeChannel.consume(process.env.SYSTEM_ID, function (content) {
          if (content !== null) {
            console.log('[NEW MESSAGE TO CONSUME] ' + content.properties.correlationId)
            consumeMessage(content)
          }
        })
      })
    } else {
      globals.consumeChannel.consume(process.env.SYSTEM_ID, function (content) {
        if (content !== null) {
          console.log('[NEW MESSAGE TO CONSUME] ' + content.properties.correlationId)
          consumeMessage(content)
        }
      })
    }
  } catch (e) {
    console.error('[AMQP consume]', e.message)
  }
}

function start () {
  globals.elastic = new elasticsearch.Client({
    host: process.env.ELASTICSEARCH_URL,
    log: 'info'
  })

  amqp.connect(process.env.AMQP_URL + '?heartbeat=60', function (err, conn) {
    if (err) {
      console.error('[AMQP connect]', err.message)
      process.exit(1)
    }
    conn.on('error', function (err) {
      if (err.message !== 'Connection closing') {
        console.error('[AMQP connect] conn error', err.message)
      }
    })
    conn.on('close', function () {
      console.error('[AMQP connect] closed')
      process.exit(1)
    })
    console.log('[AMQP connect] connected')
    globals.amqpConn = conn
    whenConnected()
  })
}

function whenConnected () {
  globals.redisClient = redis.createClient(process.env.REDIS_URL)
  globals.redisClient.on('error', function (err) {
    console.error('[REDIS] ' + err)
    return setTimeout(start, 1000)
  })
  mongoose.connect(process.env.MONGO_URL, function (err) {
    if (err) {
      console.error('[mongo]', err.message)
      process.exit(1)
    }

    globals.systemCollection = mongoose.connection.db.collection('systems')
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
