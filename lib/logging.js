//
// Copyright (c) 2018 Nathan Fiedler
//
const winston = require('winston')
const config = require('config')

// Configure the logging not related to HTTP, which is handled using morgan.
const transports = []
if (config.has('logging.file')) {
  const filename = config.get('logging.file')
  transports.push(new winston.transports.File({
    filename,
    maxsize: 1048576,
    maxFiles: 4
  }))
} else {
  transports.push(new winston.transports.Console())
}

let level = 'info'
if (config.has('logging.level')) {
  level = config.get('logging.level')
}

const logger = winston.createLogger({
  exitOnError: false,
  format: winston.format.json(),
  level,
  transports
})

module.exports = logger
