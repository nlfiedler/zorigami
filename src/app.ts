//
// Copyright (c) 2018 Nathan Fiedler
//
const createError = require('http-errors')
import * as express from 'express'
const cookieParser = require('cookie-parser')
import * as morgan from 'morgan'
require('./logging')

import * as indexRouter from './routes/index'

const app = express()

// view engine setup
app.set('views', 'views')
app.set('view engine', 'ejs')

app.use(morgan('dev'))
app.use(express.json())
app.use(express.urlencoded({ extended: false }))
app.use(cookieParser())
app.use(express.static('public'))

app.use('/', indexRouter.index)

// catch 404 and forward to error handler
app.use(function (req: express.Request, res: express.Response, next: Function) {
  next(createError(404))
})

// error handler
app.use(function (err: any, req: express.Request, res: express.Response, next: Function) {
  // set locals, only providing error in development
  res.locals.message = err.message
  res.locals.error = req.app.get('env') === 'development' ? err : {}

  // render the error page
  res.status(err.status || 500)
  res.render('error')
})

export default app
