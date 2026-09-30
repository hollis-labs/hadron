let apiBaseURL = '';

export function setAPIBaseURL(url: string) {
  apiBaseURL = url.replace(/\/$/, '');
}

export function getAPIBaseURL(): string {
  return apiBaseURL;
}

/** An API response that was not OK, with its HTTP status. */
export class APIError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'APIError';
    this.status = status;
  }
}

type UnauthorizedListener = () => void;
const unauthorizedListeners = new Set<UnauthorizedListener>();

/**
 * Subscribe to "the daemon wants a credential" (HTTP 401). The browser signs
 * in only through a single-use link from `hadron ui` or the desktop app;
 * the page itself never holds the operator token.
 */
export function onUnauthorized(listener: UnauthorizedListener): () => void {
  unauthorizedListeners.add(listener);
  return () => unauthorizedListeners.delete(listener);
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (!headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  // The session cookie is HttpOnly and SameSite=Strict; same-origin
  // credentials send it.
  const response = await fetch(`${apiBaseURL}${path}`, {
    credentials: 'same-origin',
    ...init,
    headers,
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    if (response.status === 401) {
      for (const listener of unauthorizedListeners) listener();
    }
    throw new APIError(error.error ?? error.code ?? `HTTP ${response.status}`, response.status);
  }
  return response.json() as Promise<T>;
}
