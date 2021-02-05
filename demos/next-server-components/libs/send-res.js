import { pipeToNodeWritable } from 'react-server-dom-webpack/writer.node.server'

import React from 'react'
import App from '../components/App.server'

let moduleMap
const componentRegex = /components\/.+\.js/

async function renderReactTree(props, res, moduleMap_) {
  if (!moduleMap) {
    const manifest = {}

    // We need to remap the filepaths in the manifest
    // because they have different working directory
    // inside the function.
    for (let key in moduleMap_) {
      const componentPath = key.match(componentRegex)[0]
      manifest[componentPath] = moduleMap_[key]
    }
    moduleMap = new Proxy(manifest, {
      get: function (target, prop) {
        const componentPath = prop.match(componentRegex)[0]
        return target[componentPath]
      },
    })
  }

  pipeToNodeWritable(React.createElement(App, props), res, moduleMap)
}

module.exports = async (req, res, redirectToId, moduleMap) => {
  console.time('react render')
  res.on('close', () => console.timeEnd('react render'))

  let location
  try {
    location = JSON.parse(req.query.location)
  } catch (err) {
    return res.send('Missing parameter, skipped.')
  }

  if (redirectToId) {
    location.selectedId = redirectToId
  }
  res.setHeader('X-Location', JSON.stringify(location))

  renderReactTree(
    {
      selectedId: location.selectedId,
      isEditing: location.isEditing,
      searchText: location.searchText,
      login: req.session.login || null,
    },
    res,
    moduleMap
  )
}
