import session from '../../libs/session'

const CLIENT_ID = process.env.OAUTH_CLIENT_KEY
const CLIENT_SECRET = process.env.OAUTH_CLIENT_SECRET

export default async (req, res) => {
  session(req, res)

  const { code } = req.query

  // When there's no `code` param specified,
  // it's a GET from the client side.
  // We go with the login flow.
  if (!code) {
    // Login with GitHub
    res.writeHead(302, {
      Location: `https://github.com/login/oauth/authorize?client_id=${CLIENT_ID}&allow_signup=false`,
    })
    return res.end()
  }

  try {
    console.log('fetching token from github')
    const resp =
      await fetch('https://github.com/login/oauth/access_token', {
        method: 'POST',
        body: JSON.stringify({
          client_id: CLIENT_ID,
          client_secret: CLIENT_SECRET,
          code,
        }),
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
      })
    const data = await resp.json();
    if (!resp.ok) {
      console.error(data);
      return res.status(500).send({ error: 'Failed to auth' });
    }


    const accessToken = data.access_token

    // Let's also fetch the user info and store it in the session.
    if (accessToken) {
      const userInfo = await (
        await fetch('https://api.github.com/user', {
          method: 'GET',
          withCredentials: true,
          credentials: 'include',
          headers: {
            Authorization: `token ${accessToken}`,
            Accept: 'application/json',
          },
        })
      ).json()

      console.log(`got user info from github: ${userInfo.login}`)
      req.session.login = userInfo.login
    } else {
      req.session.login = ''
    }
  } catch (err) {
    console.error(err)
    return res.status(500).send({ error: 'Failed to auth.' })
  }

  res.writeHead(302, { Location: `/` })
  res.end()
}
