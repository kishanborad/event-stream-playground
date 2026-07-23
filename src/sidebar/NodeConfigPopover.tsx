import type { SimNode, ProducerNode, TopicNode, ConsumerNode, SimulationState } from '../types';
import { rebalanceConsumers } from '../simulation/rebalance';

interface Props {
  node: SimNode;
  stateRef: React.MutableRefObject<SimulationState>;
  onClose: () => void;
}

export default function NodeConfigPopover({ node, stateRef, onClose }: Props) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="font-medium text-sm">{node.name}</span>
        <button onClick={onClose} className="text-canvas-muted hover:text-canvas-text text-xs">&#x2715;</button>
      </div>

      {node.kind === 'producer' && (
        <ProducerConfig node={node} />
      )}
      {node.kind === 'topic' && (
        <TopicConfig node={node} stateRef={stateRef} />
      )}
      {node.kind === 'consumer' && (
        <ConsumerConfig node={node} />
      )}
    </div>
  );
}

function ProducerConfig({ node }: { node: ProducerNode }) {
  return (
    <div>
      <label className="text-xs text-canvas-muted block mb-1">
        Rate: {node.messageRate} msg/s
      </label>
      <input
        type="range"
        min={1}
        max={20}
        step={1}
        value={node.messageRate}
        onChange={e => { node.messageRate = parseInt(e.target.value); }}
        className="w-full accent-canvas-producer"
      />
    </div>
  );
}

function TopicConfig({ node, stateRef }: { node: TopicNode; stateRef: React.MutableRefObject<SimulationState> }) {
  return (
    <div>
      <label className="text-xs text-canvas-muted block mb-1">
        Partitions: {node.partitionCount}
      </label>
      <input
        type="range"
        min={1}
        max={8}
        step={1}
        value={node.partitionCount}
        onChange={e => {
          const newCount = parseInt(e.target.value);
          while (node.partitions.length < newCount) {
            node.partitions.push({ produced: 0, queued: 0 });
          }
          node.partitions.length = newCount;
          node.partitionCount = newCount;
          rebalanceConsumers(stateRef.current, node.id);
        }}
        className="w-full accent-canvas-topic"
      />
    </div>
  );
}

function ConsumerConfig({ node }: { node: ConsumerNode }) {
  return (
    <div className="space-y-2">
      <div>
        <label className="text-xs text-canvas-muted block mb-1">
          Delay: {node.processingDelay}ms
        </label>
        <input
          type="range"
          min={0}
          max={500}
          step={10}
          value={node.processingDelay}
          onChange={e => { node.processingDelay = parseInt(e.target.value); }}
          className="w-full accent-canvas-consumer"
        />
      </div>
      <div>
        <label className="text-xs text-canvas-muted block mb-1">
          Failure: {Math.round(node.failureProbability * 100)}%
        </label>
        <input
          type="range"
          min={0}
          max={100}
          step={5}
          value={node.failureProbability * 100}
          onChange={e => { node.failureProbability = parseInt(e.target.value) / 100; }}
          className="w-full accent-canvas-dlq"
        />
      </div>
    </div>
  );
}
