module.exports = function () {
  return `
module.exports = {
  '__esModule': true,
  '$$typeof': Symbol.for('react.module.reference'),
  filepath: 'file://${this.resourcePath}',
  name: '*',
  defaultProps: undefined,
  default: {
    '$$typeof': Symbol.for('react.module.reference'),
    filepath: 'file://${this.resourcePath}',
    name: ''
  }
}`
}
