import { useDaemon } from '../../contexts/DaemonContext';

/**
 * Shown when the daemon answers 401: loopback is no longer a credential, so
 * the browser needs a session opened by a single-use sign-in link.
 */
export function SignInBanner() {
  const daemon = useDaemon();
  if (!daemon.signInRequired) return null;
  return (
    <div className="sign-in-banner" role="alert" data-testid="sign-in-required">
      <strong>Not signed in.</strong> Run <code>hadron ui</code> in a terminal (or start the Hadron
      app) to open a signed-in window. Sign-in links work once and expire after 60 seconds.
    </div>
  );
}
