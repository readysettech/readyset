import type { NextApiRequest, NextApiResponse } from 'next'
import {getAccessToken} from '@auth0/nextjs-auth0';
import auth0 from '../../lib/auth0'

const afterCallback = async (req: NextApiRequest, res: NextApiResponse, session, state) => {
    const {accessToken} = await getAccessToken(req, res);
    session.user.access_token = accessToken;
    return session;
};

const callback = async (req: NextApiRequest, res: NextApiResponse) => {
    try {
        await auth0.handleCallback(req, res)
    } catch (error) {
        console.error(error)
        const errorMessage =
            error instanceof Error ? error.message : 'Internal server error'
        res.status(500).end(errorMessage)
    }
}

export default callback