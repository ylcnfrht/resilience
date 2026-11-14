/** Labelled range slider with its value shown (shared by the demos). */
export default function Range(p: { label: string; value: number; min: number; max: number; step: number; fmt: (v: number) => string; set: (v: number) => void; disabled?: boolean }) {
  return (
    <label class="rp-field">
      <span>
        {p.label} <output class="rp-val">{p.fmt(p.value)}</output>
      </span>
      <input type="range" min={p.min} max={p.max} step={p.step} value={p.value} disabled={p.disabled} aria-valuetext={p.fmt(p.value)} onInput={(e) => p.set(+e.currentTarget.value)} />
    </label>
  );
}
