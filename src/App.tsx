export default function App() {
  return (
    <div className="flex h-screen bg-canvas-bg text-canvas-text overflow-hidden">
      {/* Sidebar */}
      <div className="w-64 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-r border-canvas-border flex flex-col shadow-glass">
        <div className="p-4 text-sm font-semibold border-b border-canvas-border tracking-wide text-canvas-accent">
          Event Stream
        </div>
        <div className="flex-1 p-4 text-canvas-muted text-sm">
          Controls will go here
        </div>
      </div>

      {/* Canvas area */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Toolbar */}
        <div className="h-11 bg-canvas-surface backdrop-blur-[12px] border-b border-canvas-border flex items-center px-5 text-sm">
          <span className="font-medium text-canvas-text">Event Stream Playground</span>
        </div>
        {/* Canvas */}
        <div className="flex-1 relative bg-canvas-deep">
          <div className="absolute inset-0 flex items-center justify-center text-canvas-muted">
            Canvas will render here
          </div>
        </div>
      </div>

      {/* Charts panel */}
      <div className="w-72 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-l border-canvas-border flex flex-col shadow-glass">
        <div className="p-4 text-sm font-semibold border-b border-canvas-border tracking-wide text-canvas-accent">
          Metrics
        </div>
        <div className="flex-1 p-4 text-canvas-muted text-sm">
          Charts will go here
        </div>
      </div>
    </div>
  );
}
