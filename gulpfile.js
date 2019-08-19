const { exec } = require('child_process')
const del = require('del')
const gulp = require('gulp')
const gulpif = require('gulp-if')
const uglify = require('gulp-uglify')
const webpack = require('webpack-stream')

const production = process.env.NODE_ENV === 'production'

function makebsb (cb) {
  exec('npx bsb -make-world', (err, stdout, stderr) => {
    console.info(stdout)
    console.error(stderr)
    cb(err)
  })
}

function packweb (cb) {
  return gulp.src('lib/js/src/Index.bs.js')
    .pipe(webpack({
      mode: production ? 'production' : 'development',
      output: {
        filename: 'main.js'
      }
    }))
    .pipe(gulpif(production, uglify()))
    .pipe(gulp.dest('public/javascripts'))
}

function cleanbsb (cb) {
  exec('npx bsb -clean-world', (err, stdout, stderr) => {
    console.info(stdout)
    console.error(stderr)
    cb(err)
  })
}

function cleanjs (cb) {
  return del(['public/javascripts/main.js'])
}

exports.clean = gulp.parallel(cleanbsb, cleanjs)
exports.build = gulp.series(makebsb, packweb)
