import './Waterline.css'

// The horizon the fleet sits on. Two copies of the same wave path sit
// side by side inside a double-width strip that slides left forever —
// the standard seamless-marquee trick — so the sea never visibly loops.
export default function Waterline() {
  const wave = (
    <path
      d="M0 20 Q 25 4, 50 20 T 100 20 T 150 20 T 200 20 T 250 20 T 300 20 T 350 20 T 400 20 V40 H0 Z"
      fill="url(#waterline-fill)"
    />
  )
  return (
    <div className="waterline" aria-hidden="true">
      <svg width="0" height="0">
        <defs>
          <linearGradient id="waterline-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.16" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </linearGradient>
        </defs>
      </svg>
      <div className="waterline__track">
        <svg viewBox="0 0 400 40" preserveAspectRatio="none">{wave}</svg>
        <svg viewBox="0 0 400 40" preserveAspectRatio="none">{wave}</svg>
      </div>
    </div>
  )
}
