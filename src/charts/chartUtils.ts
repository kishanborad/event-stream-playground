export function drawBackground(ctx: CanvasRenderingContext2D, w: number, h: number): void {
  ctx.fillStyle = '#0a0a1a';
  ctx.beginPath();
  ctx.roundRect(0, 0, w, h, 8);
  ctx.fill();

  ctx.strokeStyle = 'rgba(99, 102, 241, 0.06)';
  ctx.lineWidth = 0.5;
  const gridLines = 4;
  for (let i = 1; i < gridLines; i++) {
    const y = (h / gridLines) * i;
    ctx.beginPath();
    ctx.moveTo(4, y);
    ctx.lineTo(w - 4, y);
    ctx.stroke();
  }
}

export function drawLine(
  ctx: CanvasRenderingContext2D,
  points: number[],
  maxVal: number,
  w: number,
  h: number,
  color: string,
  padding: number = 4,
): void {
  if (points.length < 2 || maxVal <= 0) return;

  const drawH = h - padding * 2;
  const step = w / (points.length - 1);

  ctx.beginPath();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.lineJoin = 'round';

  for (let i = 0; i < points.length; i++) {
    const x = i * step;
    const y = padding + drawH - (points[i] / maxVal) * drawH;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

export function drawBars(
  ctx: CanvasRenderingContext2D,
  values: { label: string; value: number; color: string }[],
  maxVal: number,
  w: number,
  h: number,
  padding: number = 4,
): void {
  if (values.length === 0) return;
  const drawH = h - padding * 2 - 14;
  const barW = Math.min(30, (w - 8) / values.length - 4);
  const totalW = values.length * (barW + 4) - 4;
  const startX = (w - totalW) / 2;
  const cappedMax = maxVal > 0 ? maxVal : 1;

  for (let i = 0; i < values.length; i++) {
    const { label, value, color } = values[i];
    const barH = (value / cappedMax) * drawH;
    const x = startX + i * (barW + 4);
    const y = padding + drawH - barH;

    ctx.fillStyle = color;
    ctx.fillRect(x, y, barW, barH);

    ctx.fillStyle = '#aaa6c3';
    ctx.font = '9px Poppins, system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(label, x + barW / 2, h - 2);
  }
}

export function drawStackedArea(
  ctx: CanvasRenderingContext2D,
  seriesA: number[],
  seriesB: number[],
  maxVal: number,
  w: number,
  h: number,
  colorA: string,
  colorB: string,
  padding: number = 4,
): void {
  const len = Math.max(seriesA.length, seriesB.length);
  if (len < 2 || maxVal <= 0) return;

  const drawH = h - padding * 2;
  const step = w / (len - 1);

  // Bottom area (seriesA)
  ctx.beginPath();
  ctx.moveTo(0, h - padding);
  for (let i = 0; i < len; i++) {
    const val = (seriesA[i] ?? 0) / maxVal;
    ctx.lineTo(i * step, padding + drawH - val * drawH);
  }
  ctx.lineTo((len - 1) * step, h - padding);
  ctx.closePath();
  ctx.fillStyle = colorA + '80';
  ctx.fill();

  // Top area (seriesA + seriesB)
  ctx.beginPath();
  ctx.moveTo(0, h - padding);
  for (let i = 0; i < len; i++) {
    const val = ((seriesA[i] ?? 0) + (seriesB[i] ?? 0)) / maxVal;
    ctx.lineTo(i * step, padding + drawH - val * drawH);
  }
  ctx.lineTo((len - 1) * step, h - padding);
  ctx.closePath();
  ctx.fillStyle = colorB + '60';
  ctx.fill();
}

export function drawTitle(
  ctx: CanvasRenderingContext2D,
  title: string,
  value: string,
  w: number,
): void {
  ctx.fillStyle = '#aaa6c3';
  ctx.font = '10px Poppins, system-ui, sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(title, 4, 12);

  ctx.fillStyle = '#f4f4f6';
  ctx.textAlign = 'right';
  ctx.fillText(value, w - 4, 12);
}
