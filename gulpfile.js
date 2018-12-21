const gulp = require('gulp')
const nodemon = require('gulp-nodemon')

gulp.task('serve', (cb) => {
  let called = false
  return nodemon({
    'script': './bin/www',
    'watch': '.',
    'ext': 'js'
  }).on('start', () => {
    if (!called) {
      called = true
      cb()
    }
  })
})

gulp.task('default', gulp.series('serve'))
