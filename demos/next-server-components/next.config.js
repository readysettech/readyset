const ReactServerWebpackPlugin = require('react-server-dom-webpack/plugin')
const fs = require('fs')

let manifest
class CopyReactClientManifest {
  apply(compiler) {
    compiler.hooks.emit.tapAsync(
      'CopyReactClientManifest',
      (compilation, callback) => {
        const asset = compilation.assets['react-client-manifest.json']
        const content = asset.source()
        // there might be multiple passes (?)
        // we keep the larger manifest
        if (manifest && manifest.length > content.length) {
          callback()
          return
        }
        manifest = content
        fs.writeFile('./libs/react-client-manifest.json', content, callback)
      }
    )
  }
}

module.exports = {
  experimental: {
    reactMode: 'concurrent',
  },
  api: {
    bodyParser: false,
  },
  webpack: config => {
    config.plugins.push(new ReactServerWebpackPlugin({ isServer: false }))
    config.plugins.push(new CopyReactClientManifest())
    return config
  },
}
