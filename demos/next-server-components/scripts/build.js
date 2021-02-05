const webpack = require('webpack')
const path = require('path')

webpack(
  {
    mode: 'production',
    entry: './libs/send-res.js',
    output: {
      path: path.resolve('./libs'),
      filename: 'send-res.build.js',
      libraryTarget: 'commonjs2',
    },
    optimization: {
      // minimize: false
    },
    module: {
      rules: [
        {
          test: /\.client\.js/,
          use: {
            loader: path.resolve('./scripts/client-react-loader.js'),
          },
        },
        {
          test: /\.js$/,
          exclude: /(node_modules)/,
          use: {
            loader: 'babel-loader',
            options: {
              presets: ['@babel/preset-react'],
              plugins: ['@babel/transform-modules-commonjs'],
            },
          },
        },
      ],
    },
    stats: 'errors-only',
    target: 'node',
  },
  (err, stats) => {
    if (err) {
      console.error(err)
    }
    if (stats.hasErrors()) {
      const info = stats.toJson()
      console.error(info.errors)
    }
  }
)
