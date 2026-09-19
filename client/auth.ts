//
// Copyright (c) 2026 Nathan Fiedler
//
import { createSignal } from 'solid-js';

// The server's API_TOKEN, entered by the operator on the Settings page and
// sent as `Authorization: Bearer <token>` on every GraphQL request.
const TOKEN_KEY = 'apiToken';

export function getApiToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setApiToken(token: string) {
  try {
    if (token.length === 0) {
      localStorage.removeItem(TOKEN_KEY);
    } else {
      localStorage.setItem(TOKEN_KEY, token);
    }
  } catch (error) {
    console.error('failed to save API token:', error);
  }
  // assume the new token is good until the server says otherwise
  setUnauthorized(false);
}

// Set when the server rejects a request with 401, so the app can prompt the
// operator to enter (or fix) the API token.
const [unauthorized, setUnauthorized] = createSignal(false);
export { unauthorized, setUnauthorized };
