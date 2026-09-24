//
// Copyright (c) 2026 Nathan Fiedler
//

/**
 * Encode a list of entry names as a URL path suffix, with a leading slash,
 * or an empty string if there are no names. Each name is encoded separately
 * since entry names may contain characters that are not URL-safe.
 */
export function encodePath(names: readonly string[]): string {
  return names.map((n) => '/' + encodeURIComponent(n)).join('');
}

/**
 * Decode the value of a `*path` route parameter into a list of entry names.
 * The router supplies the parameter in its percent-encoded form.
 */
export function decodePath(splat: string | undefined): string[] {
  if (!splat) return [];
  return splat
    .split('/')
    .filter((s) => s.length > 0)
    .map((s) => {
      try {
        return decodeURIComponent(s);
      } catch {
        return s;
      }
    });
}
