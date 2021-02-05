import sendRes from './send-res.build'

const moduleMap = require('./react-client-manifest.json')

export default (...args) => sendRes(...args, moduleMap)
