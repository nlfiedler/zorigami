//
// Copyright (c) 2019 Nathan Fiedler
//
import * as express from "express"
const cookieParser = require('cookie-parser')

const router = express.Router()

router.use(express.json())
router.use(express.urlencoded({ extended: false }))
router.use(cookieParser())
router.use(express.static('public'))

router.get('/', (req: express.Request, res: express.Response) => {
  res.render('index', { title: 'Express' })
})

export default router
