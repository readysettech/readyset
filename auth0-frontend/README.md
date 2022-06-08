# Readyset Console

## What is This?

A single page app that handles customer login workflows and generates access tokens for the Readyset Installer.

## Set up Environment Variables

To connect the app with Auth0, you'll need to add the settings from your Auth0 application as environment variables

Copy the `.env.local.example` file in this directory to `.env.local` (which will be ignored by Git):

```bash
cp .env.local.example .env.local
```

Then, open `.env.local` and add the missing environment variables:

- `NEXT_PUBLIC_AUTH0_DOMAIN` - Can be found in the Auth0 dashboard under `settings`. (Should be prefixed
  with `https://`)
- `NEXT_PUBLIC_AUTH0_CLIENT_ID` - Can be found in the Auth0 dashboard under `settings`.
- `AUTH0_CLIENT_SECRET` - Can be found in the Auth0 dashboard under `settings`.
- `NEXT_PUBLIC_BASE_URL` - The base url of the application.
- `NEXT_PUBLIC_REDIRECT_URI` - The relative url path where Auth0 redirects back to.
- `NEXT_PUBLIC_POST_LOGOUT_REDIRECT_URI` - Where to redirect after logging out.
- `SESSION_COOKIE_SECRET` - A unique secret used to encrypt the cookies, has to be at least 32 characters. You can
  use [this generator](https://generate-secret.vercel.app/32) to generate a value.
- `SESSION_COOKIE_LIFETIME` - How long a session lasts in seconds. The default is 2 hours.

## Running Locally

Run the npm development script to start a local dev server that updates as changes are made to the app.

`npm run dev`