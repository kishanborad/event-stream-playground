import { useRef, useState, useCallback } from 'react';
import type { SimulationState, SimNode, PresetConfig, NodeKind } from './types';
import { createState } from './simulation/state';
import { PRESETS } from './simulation/presets';
import { addNode, removeNode } from './canvas/interaction';
import NodeGraph from './canvas/NodeGraph';
import Sidebar from './sidebar/Sidebar';

export default function App() {
  const stateRef = useRef<SimulationState>(createState(PRESETS[0]));
  const [, setRenderTick] = useState(0);
  const [selectedNode, setSelectedNode] = useState<SimNode | null>(null);
  const [currentPresetId, setCurrentPresetId] = useState(PRESETS[0].id);
  const [contextMenu, setContextMenu] = useState<{ node: SimNode; x: number; y: number } | null>(null);

  const handleRenderTick = useCallback(() => {
    setRenderTick(t => t + 1);
  }, []);

  const handlePresetChange = useCallback((preset: PresetConfig) => {
    stateRef.current = createState(preset);
    setCurrentPresetId(preset.id);
    setSelectedNode(null);
    setContextMenu(null);
  }, []);

  const handleDrop = useCallback((kind: NodeKind, x: number, y: number) => {
    addNode(stateRef.current, kind, x, y);
  }, []);

  const handleContextMenu = useCallback((node: SimNode, x: number, y: number) => {
    setContextMenu({ node, x, y });
  }, []);

  const handleRemoveNode = useCallback(() => {
    if (!contextMenu) return;
    removeNode(stateRef.current, contextMenu.node.id);
    if (selectedNode?.id === contextMenu.node.id) setSelectedNode(null);
    setContextMenu(null);
  }, [contextMenu, selectedNode]);

  return (
    <div
      className="flex h-screen bg-canvas-bg text-canvas-text overflow-hidden"
      onClick={() => setContextMenu(null)}
    >
      <Sidebar
        stateRef={stateRef}
        selectedNode={selectedNode}
        onClearSelection={() => setSelectedNode(null)}
        onPresetChange={handlePresetChange}
        currentPresetId={currentPresetId}
      />

      <div className="flex-1 flex flex-col min-w-0">
        <div className="h-11 bg-canvas-surface backdrop-blur-[12px] border-b border-canvas-border flex items-center px-5 text-sm">
          <span className="font-medium text-canvas-text">{PRESETS.find(p => p.id === currentPresetId)?.name ?? 'Custom'}</span>
          <div className="ml-6 flex items-center gap-5 text-xs text-canvas-muted">
            <span>Produced <span className="text-canvas-producer font-mono ml-1">{stateRef.current.counters.produced}</span></span>
            <span>Consumed <span className="text-canvas-success font-mono ml-1">{stateRef.current.counters.consumed}</span></span>
            <span>Failed <span className="text-canvas-dlq font-mono ml-1">{stateRef.current.counters.failed}</span></span>
          </div>
        </div>
        <div className="flex-1 relative bg-canvas-deep">
          <NodeGraph
            stateRef={stateRef}
            onRenderTick={handleRenderTick}
            onNodeClick={setSelectedNode}
            onDrop={handleDrop}
            onContextMenu={handleContextMenu}
          />

          {contextMenu && (
            <div
              className="fixed bg-canvas-surfaceSolid border border-canvas-border rounded-lg shadow-glass py-1 z-50 backdrop-blur-[12px]"
              style={{ left: contextMenu.x, top: contextMenu.y }}
              onClick={e => e.stopPropagation()}
            >
              <button
                onClick={handleRemoveNode}
                className="block w-full text-left px-4 py-2 text-sm text-canvas-dlq hover:bg-white/5 transition-colors"
              >
                Remove {contextMenu.node.name}
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Charts panel placeholder — Task 7 */}
      <div className="w-72 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-l border-canvas-border flex flex-col shadow-glass">
        <div className="p-4 text-xs font-semibold tracking-widest text-canvas-accent uppercase border-b border-canvas-border">
          Metrics
        </div>
        <div className="flex-1 p-4 text-canvas-muted text-sm">
          Charts coming in Task 7
        </div>
      </div>
    </div>
  );
}
