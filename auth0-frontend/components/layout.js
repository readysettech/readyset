import Head from 'next/head'
import Header from './header'

function Layout({user, loading = false, children}) {
    return (
        <>
            <Head>
                <title>Readyset | Console</title>
                <link rel="shortcut icon" href="/images/favicon.png"/>
            </Head>

            <Header user={user} loading={loading}/>

            <main>
                <div className="container">{children}</div>
            </main>

            <style jsx>{`
        .container {
          max-width: 42rem;
          margin: 1.5rem auto;
        }
      `}</style>
            <style jsx global>{`
            :root {
              --font-system: -apple-system, blinkmacsystemfont, 'Segoe UI', roboto, oxygen,
                ubuntu, cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
              --font-body: 'Inter', var(--font-system), sans-serif;
              --font-display: 'Space Grotesk', var(--font-body);
              --white: #fff;
            }
            
            html {
              box-sizing: border-box;
              text-rendering: geometricprecision;
              -webkit-text-size-adjust: 100%;
              -webkit-font-smoothing: antialiased;
              -moz-font-smoothing: antialiased;
              -moz-osx-font-smoothing: grayscale;
              background: #111;
            }
            
            html,
            body {
              -webkit-tap-highlight-color: transparent;
              color-scheme: dark;
            }
            
            body {
              font-family: var(--font-body);
            }
            
            @media (prefers-reduced-motion: no-preference) {
              html {
                scroll-behavior: smooth;
              }
            }
            
            *::selection {
              background: #17aeff;
              color: black;
            }
            
            .fade-in-bottom {
              opacity: 0;
              transform: translateY(10px);
            }
            
            @media (prefers-reduced-motion) {
              .fade-in-bottom {
                opacity: 1;
                transform: translateY(0);
                animation: none;
              }
            }
      `}</style>
        </>
    )
}

export default Layout
