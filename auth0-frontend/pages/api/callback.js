import auth0 from '../../lib/auth0'
import {getAccessToken} from '@auth0/nextjs-auth0';

const afterCallback = async (req, res, session, state) => {
    const {accessToken} = await getAccessToken(req, res);
    session.user.access_token = accessToken;
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
