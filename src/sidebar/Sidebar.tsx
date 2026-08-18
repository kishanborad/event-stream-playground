import { useState, lazy, Suspense } from 'react';
import type { SimulationState, SimNode, PresetConfig } from '../types';
import TabBar, { type SidebarTab } from './TabBar';
import NodePalette from './NodePalette';
import ControlPanel from './ControlPanel';
import NodeConfigPopover from './NodeConfigPopover';

const CodePanel = lazy(() => import('../scripting/CodePanel'));
const InfraPanel = lazy(() => import('../infra/InfraPanel'));

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
  selectedNode: SimNode | null;
  onClearSelection: () => void;
  onPresetChange: (preset: PresetConfig) => void;
  currentPresetId: string;
  canvasSize: { width: number; height: number };
}

export default function Sidebar({
  stateRef, selectedNode, onClearSelection, onPresetChange, currentPresetId, canvasSize,
}: Props) {
  const [activeTab, setActiveTab] = useState<SidebarTab>('visual');

  return (
    <div className="w-64 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-r border-canvas-border flex flex-col shadow-glass">
      {/* Always-visible controls */}
      <div className="p-4 border-b border-canvas-border flex-shrink-0">
        <ControlPanel
          stateRef={stateRef}
          onPresetChange={onPresetChange}
          currentPresetId={currentPresetId}
        />
      </div>

      {/* Tab bar */}
      <TabBar active={activeTab} onChange={setActiveTab} />

      {/* Tab content */}
      <div className="flex-1 overflow-y-auto">
        {activeTab === 'visual' && (
          <div className="p-4 space-y-4">
            <NodePalette />
            {selectedNode && (
              <div className="pt-3 border-t border-canvas-border">
                <NodeConfigPopover
                  node={selectedNode}
                  stateRef={stateRef}
                  onClose={onClearSelection}
                />
              </div>
            )}
          </div>
        )}

        {activeTab === 'code' && (
          <Suspense fallback={<div className="p-4 text-canvas-muted text-xs">Loading editor...</div>}>
            <CodePanel
              stateRef={stateRef}
              currentPresetId={currentPresetId}
              canvasSize={canvasSize}
            />
          </Suspense>
        )}

        {activeTab === 'deploy' && (
          <Suspense fallback={<div className="p-4 text-canvas-muted text-xs">Loading...</div>}>
            <InfraPanel stateRef={stateRef} />
          </Suspense>
        )}
      </div>
    </div>
  );
}
