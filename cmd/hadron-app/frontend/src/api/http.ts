let apiBaseURL = '';

export function setAPIBaseURL(url: string) {
  apiBaseURL = url.replace(/\/$/, '');
}

export function getAPIBaseURL(): string {
  return apiBaseURL;
}

export async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (!headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  const response = await fetch(`${apiBaseURL}${path}`, {
    ...init,
    headers,
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(error.error ?? error.code ?? `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}
