import { useEffect, useLayoutEffect, useRef, useState, type ComponentPropsWithoutRef } from "react";

const MIN_COL_PX = 72;
const MAX_NATURAL_PX = 320;
const SAMPLE_PADDING_PX = 24; // matches px-3 (12px) * 2

type Props = ComponentPropsWithoutRef<"table">;

/**
 * Measures the natural width of each column by rendering an off-screen
 * auto-layout copy of the table, then chooses between:
 *  - "auto" layout when all columns fit the available width
 *  - "fixed" layout with proportionally scaled column widths when they don't
 *
 * Keeps <thead> and <tbody> column alignment intact via a shared <colgroup>.
 */
export default function MarkdownTable(props: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const measureRef = useRef<HTMLTableElement | null>(null);
  const [colWidths, setColWidths] = useState<number[] | null>(null);
  const [layout, setLayout] = useState<"auto" | "fixed">("auto");

  const measure = () => {
    const container = containerRef.current;
    const measureTable = measureRef.current;
    if (!container || !measureTable) return;

    const headerCells = measureTable.querySelectorAll<HTMLTableCellElement>("thead th");
    const firstRowCells = measureTable.querySelectorAll<HTMLTableCellElement>("tbody tr:first-child td");
    const count = headerCells.length || firstRowCells.length;
    if (!count) return;

    // Natural width per column = max(header, first-row sample) clamped to MAX_NATURAL_PX
    const natural: number[] = [];
    for (let i = 0; i < count; i++) {
      const h = headerCells[i]?.getBoundingClientRect().width ?? 0;
      const c = firstRowCells[i]?.getBoundingClientRect().width ?? 0;
      natural.push(Math.min(MAX_NATURAL_PX, Math.max(MIN_COL_PX, Math.ceil(Math.max(h, c)) + 2)));
    }

    const available = container.clientWidth;
    const totalNatural = natural.reduce((a, b) => a + b, 0);

    if (totalNatural <= available) {
      setLayout("auto");
      setColWidths(null);
      return;
    }

    // Scale proportionally to fit container, but never below MIN_COL_PX.
    const scaled = natural.map((w) => (w / totalNatural) * available);
    const floored = scaled.map((w) => Math.max(MIN_COL_PX, Math.floor(w)));
    const sumFloored = floored.reduce((a, b) => a + b, 0);

    // If MIN_COL_PX clamping pushed us over the container, fall back to scrolling
    // with the natural widths — better than misaligned headers.
    if (sumFloored > available) {
      setLayout("auto");
      setColWidths(null);
      return;
    }

    setLayout("fixed");
    setColWidths(floored);
  };

  useLayoutEffect(() => {
    measure();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.children]);

  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(() => measure());
    ro.observe(containerRef.current);
    return () => ro.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const colCount = colWidths?.length ?? 0;

  return (
    <div ref={containerRef} className="md-table-wrap">
      {/* Off-screen auto-layout copy used purely for measuring natural column widths.
          aria-hidden so it doesn't pollute accessibility tree. */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          visibility: "hidden",
          pointerEvents: "none",
          left: 0,
          top: 0,
          width: "max-content",
          maxWidth: "none",
        }}
      >
        <table
          ref={measureRef}
          style={{
            tableLayout: "auto",
            borderCollapse: "collapse",
            fontSize: "12px",
            whiteSpace: "nowrap",
          }}
        >
          {props.children}
        </table>
      </div>

      {/* Visible, rendered table */}
      {(() => {
        const { children, style, ...rest } = props;
        return (
          <table
            {...rest}
            style={{
              ...style,
              tableLayout: layout,
              width: layout === "fixed" ? "100%" : undefined,
              minWidth: layout === "auto" ? "100%" : undefined,
            }}
          >
            {layout === "fixed" && colWidths && (
              <colgroup>
                {Array.from({ length: colCount }).map((_, i) => (
                  <col key={i} style={{ width: `${colWidths[i]}px` }} />
                ))}
              </colgroup>
            )}
            {children}
          </table>
        );
      })()}
    </div>
  );
}