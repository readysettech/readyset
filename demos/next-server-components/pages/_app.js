import Head from 'next/head'
import '../style.css'

export default function MyApp({ Component, pageProps }) {
  return (
    <>
      <Head>
        <title>React Server Components (Experimental Demo)</title>
        <meta httpEquiv="Content-Language" content="en" />
        <meta
          name="description"
          content="Experimental demo of React Server Components with Next.js. Hosted on Vercel."
        />
        <meta
          name="og:description"
          content="Experimental demo of React Server Components with Next.js. Hosted on Vercel."
        />
        <meta name="twitter:card" content="summary_large_image" />
        <meta
          name="twitter:image"
          content="https://next-server-components.vercel.app/og.png"
        />
        <meta
          name="twitter:site:domain"
          content="https://next-server-components.vercel.app"
        />
        <meta
          name="twitter:url"
          content="https://next-server-components.vercel.app/og.png"
        />
        <meta
          name="og:title"
          content="React Server Components (Experimental Demo)"
        />
        <meta
          name="og:image"
          content="https://next-server-components.vercel.app/og.png"
        />
        <link
          rel="preload"
          as="fetch"
          crossOrigin="anonymous"
          href={
            'https://next-server-components.vercel.app/api?location=%7B%22selectedId%22%3Anull%2C%22isEditing%22%3Afalse%2C%22searchText%22%3A%22%22%7D'
          }
        />
      </Head>
      <Component {...pageProps} />
    </>
  )
}
