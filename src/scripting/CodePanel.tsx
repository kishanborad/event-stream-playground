import { useState, useCallback } from 'react';
import Editor from '@monaco-editor/react';
import type { SimulationState } from '../types';
import { type ScriptLanguage, getTemplate } from './templates';
import { executePython } from './pythonBridge';
import { executeJs } from './jsBridge';
import { isPyodideReady } from './pyodideLoader';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
  currentPresetId: string;
  canvasSize: { width: number; height: number };
}

export default function CodePanel({ stateRef, currentPresetId, canvasSize }: Props) {
  const [language, setLanguage] = useState<ScriptLanguage>('python');
  const [code, setCode] = useState(() => getTemplate(currentPresetId, 'python'));
  const [output, setOutput] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const switchLanguage = useCallback((lang: ScriptLanguage) => {
    setLanguage(lang);
    setCode(getTemplate(currentPresetId, lang));
    setOutput(null);
  }, [currentPresetId]);

  const handleRun = useCallback(async () => {
    setRunning(true);
    setOutput(null);

    const result = language === 'python'
      ? await executePython(code, stateRef.current, canvasSize.width, canvasSize.height)
      : await executeJs(code, stateRef.current, canvasSize.width, canvasSize.height);

    if (result.success) {
      setOutput('Topology applied — simulation started');
    } else {
      setOutput(`Error: ${result.error}`);
    }
    setRunning(false);
  }, [code, language, stateRef, canvasSize]);

  return (
    <div className="flex flex-col h-full">
      {/* Language tabs */}
      <div className="flex border-b border-canvas-border">
        {(['python', 'javascript'] as const).map(lang => (
          <button
            key={lang}
            onClick={() => switchLanguage(lang)}
            className={`flex-1 py-2 text-[10px] font-medium uppercase tracking-wider transition-colors
              ${language === lang
                ? 'text-canvas-accent border-b border-canvas-accent'
                : 'text-canvas-muted hover:text-canvas-secondary'}`}
          >
            {lang === 'python' ? 'Python' : 'JavaScript'}
          </button>
        ))}
      </div>

      {/* Editor */}
      <div className="flex-1 min-h-0">
        <Editor
          height="100%"
          language={language}
          theme="vs-dark"
          value={code}
          onChange={v => setCode(v ?? '')}
          options={{
            minimap: { enabled: false },
            fontSize: 12,
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            fontFamily: 'monospace',
            padding: { top: 8 },
          }}
        />
      </div>

      {/* Run button + output */}
      <div className="border-t border-canvas-border p-3 space-y-2">
        <button
          onClick={handleRun}
          disabled={running}
          className="w-full py-2 rounded-lg text-sm font-medium transition-all duration-200
                     bg-canvas-accent/10 border border-canvas-accent/30 text-canvas-accent
                     hover:bg-canvas-accent/20 disabled:opacity-50"
        >
          {running
            ? (language === 'python' && !isPyodideReady() ? 'Loading Python...' : 'Running...')
            : 'Run'}
        </button>
        {output && (
          <div className={`text-xs px-2 py-1.5 rounded ${
            output.startsWith('Error') ? 'text-canvas-dlq bg-canvas-dlq/10' : 'text-canvas-success bg-canvas-success/10'
          }`}>
            {output}
          </div>
        )}
      </div>
    </div>
  );
}
