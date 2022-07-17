import React from 'react'
import {UserProvider} from '@auth0/nextjs-auth0';
import type {AppProps} from "next/app";

const App = ({ Component, pageProps }: AppProps) =>{
    // If you've used `withAuth`, pageProps.user can pre-populate the hook
    // if you haven't used `withAuth`, pageProps.user is undefined so the hook
    // fetches the user from the API routes
    const {user} = pageProps;

    return (
        <UserProvider user={user}>
            <Component {...pageProps} />
        </UserProvider>
    );
}

export default App;