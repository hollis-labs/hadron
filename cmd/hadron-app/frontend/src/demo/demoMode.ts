// Global demo mode state — when enabled, API calls return mock data
let _demoMode = false;
const _listeners = new Set<(enabled: boolean) => void>();

export function isDemoMode(): boolean { return _demoMode; }

export function setDemoMode(enabled: boolean) {
  _demoMode = enabled;
  _listeners.forEach(fn => fn(enabled));
}

export function onDemoModeChange(fn: (enabled: boolean) => void): () => void {
  _listeners.add(fn);
  return () => { _listeners.delete(fn); };
}
