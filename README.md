# Event Stream Playground

A browser-based Kafka-style event streaming visualization. Build topologies by dragging producers, topics, and consumers onto a canvas, write real Python or JavaScript code to configure them, and monitor everything through live charts. No server, no API keys, runs entirely in the browser.

**Live demo:** [kishanborad.github.io/event-stream-playground](https://kishanborad.github.io/event-stream-playground/)

## What it does

Three ways to build topologies:

- **Visual** -- drag Producer, Topic, and Consumer nodes onto the canvas from the sidebar. Configure message rate, processing delay, failure probability, and partition count per node.
- **Code** -- write real Python (kafka-python) or JavaScript (kafkajs) scripts in the built-in editor. Python runs in-browser via Pyodide. Hit Run and the topology builds itself.
- **Deploy** -- see the auto-generated Docker Compose file and Kafka CLI commands for the current topology. Copy and use them in a real environment.

The center canvas shows an animated node graph with colored particles flowing along bezier curves. Blue for normal messages, orange for retries, red for dead letters, green flash on successful consumption. Four live metrics charts on the right track messages/sec, consumer lag, consumer offsets, and retry/DLQ depth.

## Preset scenarios

| Preset | What it demonstrates |
|--------|---------------------|
| Happy Path | Balanced throughput -- 2 producers, 3 partitions, 3 consumers, zero failures |
| Consumer Lag | One slow consumer can't keep up -- lag grows visibly |
| Crash & Rebalance | Consumer crashes at 10s, partitions rebalance, recovers at 20s |
| Retry & DLQ | 30% failure rate -- watch retries and dead letters accumulate |

## Getting started

```bash
git clone https://github.com/kishanborad/event-stream-playground.git
cd event-stream-playground
npm install
npm run dev
```

Open `http://localhost:5174`.

## Scripts

```bash
npm run dev        # Start dev server
npm run build      # Production build
npm run preview    # Preview production build
npm test           # Run unit tests (Vitest)
npm run typecheck  # TypeScript check
npm run deploy     # Deploy to GitHub Pages
```

## Tech stack

- React 18 + TypeScript
- Vite 5 + Tailwind CSS 3
- Python (Pyodide -- real Python running in the browser via WASM)
- JavaScript (kafkajs API patterns)
- Apache Kafka (simulated partitions, consumer groups, rebalancing, DLQ)
- Docker (auto-generated docker-compose.yml from live topology)
- Bash (auto-generated Kafka CLI commands)
- HTML5 Canvas 2D (node graph, particle animation, all 4 charts)
- Monaco Editor (code editing with syntax highlighting)
- Vitest (unit tests)
- GitHub Pages (hosting)

## Project structure

```
src/
  simulation/         # Core simulation engine
    state.ts            State factory and node helpers
    engine.ts           Tick-based simulation loop
    retry.ts            Retry queue and DLQ logic
    rebalance.ts        Consumer partition rebalancing
    presets.ts          Four preset scenario configurations
  canvas/             # Visual rendering
    NodeGraph.tsx       Canvas component with rAF loop
    renderer.ts         Node, connection, and particle drawing
    layout.ts           Three-column auto-layout
    interaction.ts      Add/remove node helpers
  sidebar/            # Left panel
    Sidebar.tsx         Tab container (Visual / Code / Deploy)
    TabBar.tsx          Tab navigation
    NodePalette.tsx     Draggable node icons
    ControlPanel.tsx    Play/pause, speed, presets, reset
    NodeConfigPopover.tsx  Per-node settings
  scripting/          # Code execution
    CodePanel.tsx       Monaco editor with Python/JS tabs
    pyodideLoader.ts    Lazy Pyodide WASM loader
    pythonBridge.ts     Mock kafka-python module
    jsBridge.ts         Mock kafkajs module
    templates.ts        Code templates per preset
  infra/              # Infrastructure generation
    InfraPanel.tsx      Docker/CLI preview panel
    dockerCompose.ts    docker-compose.yml generator
    kafkaCli.ts         Kafka CLI command generator
  charts/             # Metrics visualization
    ChartPanel.tsx      Four-chart container
    MessagesPerSecChart.tsx
    ConsumerLagChart.tsx
    ConsumerOffsetChart.tsx
    RetryDlqChart.tsx
    chartUtils.ts       Canvas 2D drawing primitives
  toolbar/
    Toolbar.tsx         Top bar with counters
  types.ts            # Shared TypeScript types
  App.tsx             # Root component
```

## AI tools

Built with [Claude Code](https://claude.ai/code) as the AI copilot for code generation, agent-driven development, and automated testing workflows.

## Author

Kishan Borad
- [GitHub](https://github.com/kishanborad)
- [LinkedIn](https://linkedin.com/in/kishanborad27)

## License

MIT -- see [LICENSE](LICENSE).
