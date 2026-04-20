

## Updated KPI Dashboard Cards

Replace the 6 shipping/logistics KPI cards with 8 ecommerce-style KPI cards in a responsive grid (2-col mobile, 4-col desktop):

| Card | Metric | Indicator |
|------|--------|-----------|
| Total Orders | 12,847 | +8.2% trend arrow |
| Revenue | $1.24M | +12.5% growth |
| Conversion Rate | 3.8% | +0.4% trend |
| Cart Abandonment | 68.2% | warning badge |
| Top Products | "Wireless Pro Max" | 2,341 units |
| Active Users | 1,429 live | +15% trend |
| Average Order Value | $96.40 | +3.2% change |
| Return Rate | 4.1% | alert badge |

**Card styling**: Each card gets a dark glassmorphism container (`bg-white/5 backdrop-blur-md border border-white/10`), rounded-xl corners, soft shadow, hover glow + scale transform, with a Lucide icon, bold metric value, and colored trend/alert indicator (green for positive, red for negative/warning, amber for alerts).

**Files changed**: `src/pages/Index.tsx` — update the KPI cards array and grid to render 8 cards instead of 6, with the new ecommerce data and indicators.

