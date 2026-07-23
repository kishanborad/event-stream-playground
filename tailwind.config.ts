import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Poppins', 'system-ui', 'sans-serif'],
      },
      colors: {
        canvas: {
          bg: '#050816',
          deep: '#0a0a1a',
          surface: 'rgba(20, 20, 30, 0.7)',
          surfaceSolid: '#0d0d1a',
          border: 'rgba(255, 255, 255, 0.08)',
          borderHover: 'rgba(255, 255, 255, 0.15)',
          text: '#f4f4f6',
          secondary: '#aaa6c3',
          muted: '#64648a',
          accent: '#818cf8',
          accentDim: '#6366f1',
          producer: '#22c55e',
          topic: '#6366f1',
          consumer: '#f59e0b',
          retry: '#f97316',
          dlq: '#ef4444',
          success: '#22c55e',
          glow: 'rgba(99, 102, 241, 0.15)',
        },
      },
      boxShadow: {
        glass: '0 8px 32px rgba(0, 0, 0, 0.3)',
        glow: '0 0 20px rgba(99, 102, 241, 0.1)',
      },
      backdropBlur: {
        glass: '12px',
      },
    },
  },
  plugins: [],
} satisfies Config;
