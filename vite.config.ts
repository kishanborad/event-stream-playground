import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: '/event-stream-playground/',
  plugins: [react()],
  server: {
    port: 5174,
  },
});
