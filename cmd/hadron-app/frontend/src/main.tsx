import { applyTheme, getInitialTheme } from '@hollis-labs/sysop-ui/ui'
import ReactDOM from 'react-dom/client'
import App from './App'
import './assets/fonts/fonts.css'
import './tokens.css'
import './index.css'

applyTheme(getInitialTheme())

ReactDOM.createRoot(document.getElementById('root')!).render(
  <App />
)
