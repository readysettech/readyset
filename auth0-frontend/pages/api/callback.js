import auth0 from '../../lib/auth0'

const afterCallback = (req, res, session, state) => {
    session.user.access_token = session.accessToken;
    return session;
};

export default async function callback(req, res) {
    try {
        await auth0.handleCallback(req, res, {afterCallback})
    } catch (error) {
        console.error(error)
        res.status(error.status || 500).end(error.message)
    }
}
