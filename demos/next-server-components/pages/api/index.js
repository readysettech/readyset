import sendRes from '../../libs/send-res-with-module-map'
import session from '../../libs/session'

export default async (req, res) => {
  session(req, res)

  // if `id` is undefined, it points to /react endpoint
  if (req.method !== 'GET') {
    return res.send('Method not allowed.')
  }

  sendRes(req, res, null)
}
