import { describe, it, expect } from 'vitest';
import { PRESETS, getPreset } from '../simulation/presets';
import type { ConsumerNode } from '../types';

describe('PRESETS', () => {
  it('has 4 presets', () => {
    expect(PRESETS).toHaveLength(4);
  });

  it('each preset builds valid state with nodes and connections', () => {
    for (const preset of PRESETS) {
      const { nodes, connections } = preset.build();
      expect(nodes.length).toBeGreaterThan(0);
      expect(connections.length).toBeGreaterThan(0);

      const hasProducer = nodes.some(n => n.kind === 'producer');
      const hasTopic = nodes.some(n => n.kind === 'topic');
      const hasConsumer = nodes.some(n => n.kind === 'consumer');
      expect(hasProducer).toBe(true);
      expect(hasTopic).toBe(true);
      expect(hasConsumer).toBe(true);
    }
  });

  it('happy-path has 0% failure rate', () => {
    const preset = getPreset('happy-path');
    const { nodes } = preset.build();
    const consumers = nodes.filter(n => n.kind === 'consumer') as ConsumerNode[];
    for (const c of consumers) {
      expect(c.failureProbability).toBe(0);
    }
  });

  it('consumer-lag has slow consumer', () => {
    const preset = getPreset('consumer-lag');
    const { nodes } = preset.build();
    const consumers = nodes.filter(n => n.kind === 'consumer') as ConsumerNode[];
    expect(consumers).toHaveLength(1);
    expect(consumers[0].processingDelay).toBe(200);
  });

  it('crash-rebalance has scripted events', () => {
    const preset = getPreset('crash-rebalance');
    const { scriptedEvents } = preset.build();
    expect(scriptedEvents.length).toBe(2);
    expect(scriptedEvents[0].action).toBe('crash');
    expect(scriptedEvents[1].action).toBe('recover');
  });

  it('retry-dlq has a consumer with 30% failure', () => {
    const preset = getPreset('retry-dlq');
    const { nodes } = preset.build();
    const consumers = nodes.filter(n => n.kind === 'consumer') as ConsumerNode[];
    const failing = consumers.find(c => c.failureProbability > 0);
    expect(failing).toBeDefined();
    expect(failing!.failureProbability).toBe(0.3);
  });
});

describe('getPreset', () => {
  it('returns preset by id', () => {
    expect(getPreset('happy-path').name).toBe('Happy Path');
  });

  it('falls back to first preset for unknown id', () => {
    expect(getPreset('unknown')).toBe(PRESETS[0]);
  });
});
