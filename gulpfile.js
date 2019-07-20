const { exec } = require('child_process')
const del = require('del')
const gulp = require('gulp')
const gulpif = require('gulp-if')
const uglify = require('gulp-uglify')
const webpack = require('webpack-stream')

const production = process.env.NODE_ENV === 'production'

gulp.task('make:rust', (cb) => {
  exec('cargo build', (err, stdout, stderr) => {
    console.info(stdout)
    console.error(stderr)
    cb(err)
  })
})

gulp.task('make:bsb', (cb) => {
  exec('npx bsb -make-world', (err, stdout, stderr) => {
    console.info(stdout)
    console.error(stderr)
    cb(err)
  })
})

gulp.task('webpack', () => {
  return gulp.src('lib/js/src/Index.bs.js')
    .pipe(webpack({
      mode: production ? 'production' : 'development',
      output: {
        filename: 'main.js'
      }
    }))
    .pipe(gulpif(production, uglify()))
    .pipe(gulp.dest('public/javascripts'))
})

gulp.task('clean:bsb', (cb) => {
  exec('npx bsb -clean-world', (err, stdout, stderr) => {
    console.info(stdout)
    console.error(stderr)
    cb(err)
  })
})

gulp.task('clean:js', () => {
  return del(['public/javascripts/main.js'])
})

gulp.task('clean', gulp.series('clean:bsb', 'clean:js'))
gulp.task('default', gulp.series('make:rust', 'make:bsb', 'webpack'))
