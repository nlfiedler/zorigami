const fx = require('fs-extra')
const gulp = require('gulp')
const nodemon = require('gulp-nodemon')
const ts = require('gulp-typescript')
const tsProject = ts.createProject('tsconfig.json')

gulp.task('compile', () => {
  return tsProject.src()
    .pipe(tsProject())
    .js.pipe(gulp.dest('dist'))
})

gulp.task('serve', (cb) => {
  let called = false
  return nodemon({
    'script': './dist/server.js',
    'watch': './dist',
    'ext': 'js'
  }).on('start', () => {
    if (!called) {
      called = true
      cb()
    }
  })
})

gulp.task('js-clean', (cb) => {
  fx.remove('dist', err => {
    cb(err)
  })
})

gulp.task('watch-server', () => {
  gulp.watch('src/**/*.ts', gulp.series('compile'))
})

gulp.task('clean', gulp.series('js-clean'))
gulp.task('default', gulp.series('compile', 'serve', 'watch-server'))
