declare global {
  interface Window {
    loadPyodide?: (config: { indexURL: string }) => Promise<PyodideInterface>;
  }
}

interface PyodideInterface {
  runPythonAsync: (code: string) => Promise<unknown>;
  registerJsModule: (name: string, module: Record<string, unknown>) => void;
  globals: { get: (name: string) => unknown };
}

const PYODIDE_CDN = 'https://cdn.jsdelivr.net/pyodide/v0.26.0/full/';
let pyodide: PyodideInterface | null = null;
let loading = false;
let loadPromise: Promise<PyodideInterface> | null = null;

export async function getPyodide(): Promise<PyodideInterface> {
  if (pyodide) return pyodide;
  if (loadPromise) return loadPromise;

  loading = true;
  loadPromise = (async () => {
    // Load Pyodide script from CDN if not already present
    if (!window.loadPyodide) {
      const script = document.createElement('script');
      script.src = `${PYODIDE_CDN}pyodide.js`;
      document.head.appendChild(script);
      await new Promise<void>((resolve, reject) => {
        script.onload = () => resolve();
        script.onerror = () => reject(new Error('Failed to load Pyodide'));
      });
    }

    pyodide = await window.loadPyodide!({ indexURL: PYODIDE_CDN });
    loading = false;
    return pyodide;
  })();

  return loadPromise;
}

export function isPyodideLoading(): boolean {
  return loading;
}

export function isPyodideReady(): boolean {
  return pyodide !== null;
}
