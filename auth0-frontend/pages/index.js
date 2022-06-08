import Layout from '../components/layout'
import {Prism as SyntaxHighlighter} from 'react-syntax-highlighter'
import {xonokai} from 'react-syntax-highlighter/dist/cjs/styles/prism'
import {CopyToClipboard} from "react-copy-to-clipboard";
import {useUser} from '@auth0/nextjs-auth0';


function Home() {
    const {user, error, isLoading} = useUser();

    return (
        <Layout user={user} loading={isLoading}>
            <h1>Readyset Console</h1>

            {isLoading && <p>Loading login info...</p>}

            {error && (
                <>
                    <h4>Error</h4>
                    <pre>{error.message}</pre>
                </>
            )}

            {!isLoading && !error && !user && (
                <>
                    <p>
                        Click <i>Login</i> to setup an account and download the Readyset Free Tier Installer.
                    </p>
                    <p>
                        Click on <i>Getting Started</i> to see a demo of using Readyset Free Tier.
                    </p>
                </>
            )}

            {user && (
                <>
                    <p>Hi {user.name}! Your customized command to run the Readyset Installer is:</p>
                    <SyntaxHighlighter language="bash" style={xonokai} wrapLongLines={true}>
                        {`RS_API_KEY=${user.access_token} bash -c "$(curl -sSL https://launch.readyset.io)"`}
                    </SyntaxHighlighter>
                    <CopyToClipboard
                        text={`RS_API_KEY=${user.access_token} bash -c "$(curl -sSL https://launch.readyset.io)"`}>
                        <button>
                            <i className="fa fa-clipboard" aria-hidden="true"/> Copy to Clipboard
                        </button>
                    </CopyToClipboard>
                </>
            )}
        </Layout>
    )
}

export default Home
