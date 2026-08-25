import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    // The daemon is the production web host. Keeping the generated bundle in
    // an internal Go package lets ordinary `go build ./cmd/hadrond` embed the
    // exact same SPA used by `vite preview` without depending on Wails.
    outDir: '../../../internal/webui/dist',
    emptyOutDir: true,
  },
  server: {
    port: 34116,
    proxy: {
      '/v1': {
        target: 'http://127.0.0.1:8095',
        changeOrigin: true,
      },
    },
  },
})
