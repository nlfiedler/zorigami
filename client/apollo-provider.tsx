//
// Copyright (c) 2026 Nathan Fiedler
//
import { createContext, useContext } from 'solid-js';
import {
  ApolloClient,
  ApolloLink,
  HttpLink,
  InMemoryCache,
  ServerError
} from '@apollo/client';
import { SetContextLink } from '@apollo/client/link/context';
import { ErrorLink } from '@apollo/client/link/error';
import { getApiToken, setUnauthorized } from './auth.ts';

const ApolloContext = createContext<ApolloClient | undefined>();

export function ApolloProvider(props: { children: any }) {
  // attach the API token (if any) to every request
  const authLink = new SetContextLink((prevContext) => {
    const token = getApiToken();
    if (!token) {
      return {};
    }
    return {
      headers: { ...prevContext.headers, authorization: `Bearer ${token}` }
    };
  });
  // flag a rejected token so the app can prompt for a new one
  const errorLink = new ErrorLink(({ error }) => {
    if (ServerError.is(error) && error.statusCode === 401) {
      setUnauthorized(true);
    }
  });
  const client = new ApolloClient({
    link: ApolloLink.from([
      errorLink,
      authLink,
      new HttpLink({ uri: '/graphql' })
    ]),
    // a cache is required, but caching can be disabled
    cache: new InMemoryCache(),
    defaultOptions: {
      query: {
        fetchPolicy: 'no-cache',
        errorPolicy: 'all'
      },
      watchQuery: {
        fetchPolicy: 'no-cache',
        errorPolicy: 'all'
      }
    }
  });
  return (
    <ApolloContext.Provider value={client}>
      {props.children}
    </ApolloContext.Provider>
  );
}

export function useApolloClient() {
  const client = useContext(ApolloContext);
  if (!client) {
    throw new Error('useApolloClient must be used within an ApolloProvider');
  }
  return client;
}
