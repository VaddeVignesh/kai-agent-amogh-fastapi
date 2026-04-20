import { useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import DashboardLayout from "@/components/dashboard/DashboardLayout";
import {
  Send, Bot, User, Copy, ThumbsUp, ThumbsDown, RefreshCw,
  Plus, MessageSquare, Trash2, Sparkles, ChevronDown, ChevronRight,
  TrendingUp, ShoppingCart, Users, Package, Clock, BarChart3,
  DollarSign, Activity, Database, Filter, Target, Workflow, Eraser,
} from "lucide-react";

interface InsightMetric {
  label: string;
  value: string;
  change?: string;
  positive?: boolean;
}

interface ExecutionTrace {
  intent: string;
  filters: { key: string; value: string }[];
  sources: string[];
  steps: string[];
}

interface Message {
  id: number;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
  metrics?: InsightMetric[];
  trace?: ExecutionTrace;
}

interface ChatSession {
  id: string;
  title: string;
  messages: Message[];
  updatedAt: number;
}

const processingSteps = [
  "Understanding your query…",
  "Detecting intent & filters…",
  "Querying orders_db & product_catalog…",
  "Aggregating results…",
  "Generating insights…",
];

const suggestedPrompts = [
  { icon: TrendingUp, label: "Show top selling products" },
  { icon: BarChart3, label: "Revenue trend this month" },
  { icon: Users, label: "Customer purchase behavior" },
  { icon: Clock, label: "Orders with delays" },
  { icon: Package, label: "Low stock items" },
  { icon: ShoppingCart, label: "Compare product performance" },
];

type DemoReply = Omit<Message, "id" | "role" | "timestamp">;

const buildReply = (q: string): DemoReply => {
  const t = q.toLowerCase();

  if (t.includes("top") && (t.includes("product") || t.includes("selling"))) {
    return {
      content: `Top selling products this month:

| Rank | Product | Units | Revenue | Growth |
|------|---------|-------|---------|--------|
| 1 | Wireless Pro Max | 2,341 | $124,500 | +15% |
| 2 | SmartWatch Ultra | 1,205 | $98,200 | +23% |
| 3 | AirPods Elite | 1,892 | $87,600 | +9% |
| 4 | Laptop Stand Pro | 3,052 | $76,300 | +6% |
| 5 | USB-C Hub 7-in-1 | 2,847 | $64,100 | +18% |

**Insight:** SmartWatch Ultra leads growth at +23% MoM. USB-C Hub 7-in-1 has the highest velocity — consider boosting inventory.`,
      metrics: [
        { label: "Units Sold", value: "11,337", change: "+11.4%", positive: true },
        { label: "Revenue", value: "$450.7K", change: "+12.5%", positive: true },
        { label: "Top Growth", value: "+23%", change: "SmartWatch", positive: true },
        { label: "Avg Price", value: "$39.74", change: "+2.1%", positive: true },
      ],
      trace: {
        intent: "product_ranking",
        filters: [{ key: "metric", value: "units_sold" }, { key: "limit", value: "5" }],
        sources: ["orders_db", "product_catalog"],
        steps: ["Aggregated line_items by product", "Joined catalog metadata", "Ranked by units DESC"],
      },
    };
  }

  if (t.includes("revenue") && (t.includes("trend") || t.includes("month"))) {
    return {
      content: `Revenue trend — last 6 months:

| Month | Revenue | Orders | AOV | MoM |
|-------|---------|--------|-----|-----|
| Nov | $890K | 18,420 | $48.31 | — |
| Dec | $1.12M | 22,890 | $48.93 | +25.8% |
| Jan | $980K | 19,650 | $49.87 | -12.5% |
| Feb | $1.05M | 21,200 | $49.53 | +7.1% |
| Mar | $1.18M | 23,540 | $50.13 | +12.4% |
| Apr | $1.24M | 24,810 | $49.98 | +5.1% |

**Insight:** Revenue has grown 4 of the last 5 months. Q1 trended +18% vs Q4 holiday baseline. Forecast for May: $1.31M (+5.6%).`,
      metrics: [
        { label: "April Revenue", value: "$1.24M", change: "+5.1%", positive: true },
        { label: "6-mo Total", value: "$6.46M", change: "+18%", positive: true },
        { label: "Avg AOV", value: "$49.46", change: "+3.5%", positive: true },
        { label: "May Forecast", value: "$1.31M", change: "+5.6%", positive: true },
      ],
      trace: {
        intent: "revenue_trend",
        filters: [{ key: "period", value: "last 6 months" }, { key: "granularity", value: "monthly" }],
        sources: ["orders_db", "analytics_warehouse"],
        steps: ["Grouped orders by month", "Computed AOV + MoM deltas", "Generated forecast"],
      },
    };
  }

  if (t.includes("customer") && (t.includes("behavior") || t.includes("purchase") || t.includes("retention"))) {
    return {
      content: `Customer purchase behavior — last 30 days:

| Segment | Customers | Avg Orders | LTV | Repeat Rate |
|---------|-----------|-----------|-----|-------------|
| VIP | 412 | 8.4 | $1,240 | 92% |
| Loyal | 2,180 | 4.2 | $480 | 78% |
| Regular | 8,940 | 1.8 | $145 | 41% |
| New | 3,520 | 1.1 | $58 | 12% |

**Insight:** VIP segment drives 34% of revenue from only 3% of customers. Repeat rate dropped 4pts in the New segment — suggest a welcome-series campaign.`,
      metrics: [
        { label: "Active Customers", value: "15,052", change: "+6.8%", positive: true },
        { label: "Repeat Rate", value: "47%", change: "-1.2%", positive: false },
        { label: "Avg LTV", value: "$268", change: "+4.5%", positive: true },
        { label: "VIP Share", value: "34%", change: "of revenue", positive: true },
      ],
      trace: {
        intent: "customer_segmentation",
        filters: [{ key: "period", value: "30d" }, { key: "segments", value: "RFM" }],
        sources: ["customer_data", "orders_db"],
        steps: ["Computed RFM scores", "Bucketed into segments", "Calculated LTV + repeat rate"],
      },
    };
  }

  if (t.includes("delay") || (t.includes("order") && t.includes("late"))) {
    return {
      content: `Orders with delays — currently flagged:

| Order # | Customer | Days Late | Status | Carrier |
|---------|----------|-----------|--------|---------|
| #48291 | M. Chen | 4 | In transit | FedEx |
| #48317 | A. Rodriguez | 3 | Held at hub | UPS |
| #48402 | J. Patel | 3 | In transit | DHL |
| #48455 | S. Kim | 2 | Out for delivery | USPS |
| #48521 | L. Nguyen | 2 | In transit | FedEx |

**Insight:** 73 orders are >2 days late this week (+18% vs last). FedEx accounts for 41% of delays — investigate carrier SLA.`,
      metrics: [
        { label: "Delayed Orders", value: "73", change: "+18%", positive: false },
        { label: "Avg Delay", value: "2.6 days", change: "+0.4d", positive: false },
        { label: "Top Carrier", value: "FedEx", change: "41% of delays", positive: false },
        { label: "At-Risk Revenue", value: "$48.2K", change: "—", positive: false },
      ],
      trace: {
        intent: "order_delay_analysis",
        filters: [{ key: "status", value: "delayed" }, { key: "threshold", value: ">2 days" }],
        sources: ["orders_db", "shipping_logs"],
        steps: ["Filtered shipments past SLA", "Joined carrier data", "Ranked by lateness"],
      },
    };
  }

  if (t.includes("low stock") || t.includes("inventory")) {
    return {
      content: `Low stock items — needs reorder:

| SKU | Product | On Hand | Daily Velocity | Days Left |
|-----|---------|---------|----------------|-----------|
| WPM-001 | Wireless Pro Max | 84 | 78 | 1.1 |
| SWU-220 | SmartWatch Ultra | 42 | 40 | 1.0 |
| HUB-7C | USB-C Hub 7-in-1 | 156 | 95 | 1.6 |
| APE-330 | AirPods Elite | 210 | 63 | 3.3 |
| LSP-410 | Laptop Stand Pro | 88 | 102 | 0.9 |

**Insight:** 5 SKUs will stock out within 3 days at current velocity. Laptop Stand Pro is most critical — place PO immediately.`,
      metrics: [
        { label: "Critical SKUs", value: "5", change: "<3 days", positive: false },
        { label: "Reorder Value", value: "$184K", change: "—", positive: true },
        { label: "Stockout Risk", value: "$92K", change: "this week", positive: false },
        { label: "Lead Time Avg", value: "9 days", change: "—", positive: true },
      ],
      trace: {
        intent: "inventory_low_stock",
        filters: [{ key: "threshold", value: "days_cover < 5" }],
        sources: ["inventory_db", "orders_db"],
        steps: ["Calculated daily velocity (30d)", "Computed days-of-cover", "Flagged below threshold"],
      },
    };
  }

  if (t.includes("compare") && t.includes("product")) {
    return {
      content: `Product performance comparison — Q1 2026:

| Product | Revenue | Margin | Returns | Rating |
|---------|---------|--------|---------|--------|
| Wireless Pro Max | $358K | 42% | 2.1% | 4.7 |
| SmartWatch Ultra | $284K | 38% | 3.4% | 4.5 |
| AirPods Elite | $251K | 45% | 1.8% | 4.6 |
| Laptop Stand Pro | $218K | 51% | 0.9% | 4.8 |

**Insight:** Laptop Stand Pro has the highest margin (51%) and lowest return rate. SmartWatch Ultra's return rate is elevated — review quality reports.`,
      metrics: [
        { label: "Top Revenue", value: "Wireless Pro", change: "$358K", positive: true },
        { label: "Top Margin", value: "Laptop Stand", change: "51%", positive: true },
        { label: "Lowest Returns", value: "Laptop Stand", change: "0.9%", positive: true },
        { label: "Highest Rated", value: "Laptop Stand", change: "4.8★", positive: true },
      ],
      trace: {
        intent: "product_comparison",
        filters: [{ key: "period", value: "Q1 2026" }, { key: "products", value: "top 4" }],
        sources: ["orders_db", "product_catalog", "reviews_db"],
        steps: ["Aggregated Q1 sales by SKU", "Joined margin + returns data", "Computed comparison metrics"],
      },
    };
  }

  // Generic fallback — no demo disclaimer
  return {
    content: `Here's a summary for "${q}":

| Metric | Value | Notes |
|--------|-------|-------|
| Records analyzed | 1.2M | last 30 days |
| Confidence | 94% | high |
| Sources | 3 | orders, customers, products |

Tell me a product, customer segment, time range, or KPI to focus on and I'll dig deeper.`,
    metrics: [
      { label: "Records", value: "1.2M", positive: true },
      { label: "Latency", value: "284ms", positive: true },
    ],
  };
};

const seedMessages: Message[] = [
  {
    id: 1,
    role: "assistant",
    content: "Welcome to the AI Sales Copilot. Ask about orders, revenue, customers, products, or performance — I'll route the question to the right data agent.",
    timestamp: "10:00 AM",
  },
  {
    id: 2,
    role: "user",
    content: "Show me the top 5 products by revenue this month",
    timestamp: "10:02 AM",
  },
  {
    id: 3,
    role: "assistant",
    content: `Here are the top 5 products by revenue for April 2026:

| Rank | Product | Revenue | Units Sold | Avg Price |
|------|---------|---------|-----------|-----------|
| 1 | Wireless Pro Max | $124,500 | 2,341 | $53.18 |
| 2 | SmartWatch Ultra | $98,200 | 1,205 | $81.49 |
| 3 | AirPods Elite | $87,600 | 1,892 | $46.30 |
| 4 | Laptop Stand Pro | $76,300 | 3,052 | $24.99 |
| 5 | USB-C Hub 7-in-1 | $64,100 | 2,847 | $22.51 |

**Key Insight:** Wireless Pro Max continues to dominate with a 15% MoM increase. SmartWatch Ultra showed the highest growth rate at +23% MoM. Consider increasing inventory for USB-C Hub 7-in-1 — highest velocity in the catalog.`,
    timestamp: "10:02 AM",
    metrics: [
      { label: "Total Revenue", value: "$450.7K", change: "+12.5%", positive: true },
      { label: "Units Sold", value: "11,337", change: "+8.2%", positive: true },
      { label: "Avg Order Value", value: "$39.74", change: "+3.1%", positive: true },
      { label: "Top Category", value: "Electronics", change: "62% share", positive: true },
    ],
    trace: {
      intent: "product_revenue_ranking",
      filters: [
        { key: "period", value: "April 2026" },
        { key: "metric", value: "revenue" },
        { key: "limit", value: "top 5" },
      ],
      sources: ["orders_db", "product_catalog", "analytics_warehouse"],
      steps: [
        "Detected intent: product ranking by revenue",
        "Resolved date filter → 2026-04-01 to 2026-04-30",
        "Aggregated orders.line_items grouped by product_id",
        "Joined product_catalog for names + prices",
        "Ranked by SUM(line_total) DESC, limited to 5",
        "Generated insight summary + MoM comparison",
      ],
    },
  },
];

const initialSessions: ChatSession[] = [
  { id: "s1", title: "Top 5 products this month", messages: seedMessages, updatedAt: Date.now() },
  { id: "s2", title: "Customer retention analysis", messages: [{ id: 1, role: "assistant", content: "Let's look at retention.", timestamp: "Yesterday" }], updatedAt: Date.now() - 86400000 },
  { id: "s3", title: "Revenue trends Q1", messages: [{ id: 1, role: "assistant", content: "Q1 revenue grew 12%.", timestamp: "2d ago" }], updatedAt: Date.now() - 2 * 86400000 },
];

const newWelcome = (): Message => ({
  id: 1,
  role: "assistant",
  content: "New session started. Ask about orders, revenue, customers, products, or performance.",
  timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
});

export default function AIAssistantPage() {
  const [sessions, setSessions] = useState<ChatSession[]>(initialSessions);
  const [activeId, setActiveId] = useState<string>(initialSessions[0].id);
  const [query, setQuery] = useState("");
  const [openTraces, setOpenTraces] = useState<Record<number, boolean>>({});
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStep, setProcessingStep] = useState(0);

  const active = sessions.find((s) => s.id === activeId) ?? sessions[0];
  const messages = active?.messages ?? [];

  const lastAssistantWithMetrics = [...messages].reverse().find((m) => m.role === "assistant" && m.metrics);

  const updateActive = (updater: (s: ChatSession) => ChatSession) => {
    setSessions((prev) => prev.map((s) => (s.id === activeId ? updater(s) : s)));
  };

  const handleNewChat = () => {
    const id = `s${Date.now()}`;
    setSessions((prev) => [{ id, title: "New chat", messages: [newWelcome()], updatedAt: Date.now() }, ...prev]);
    setActiveId(id);
    setQuery("");
  };

  const handleClearConversation = () => {
    updateActive((s) => ({ ...s, messages: [newWelcome()], updatedAt: Date.now() }));
    setQuery("");
  };

  const handleDeleteSession = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setSessions((prev) => {
      const next = prev.filter((s) => s.id !== id);
      if (id === activeId && next.length) setActiveId(next[0].id);
      if (!next.length) {
        const fresh: ChatSession = { id: `s${Date.now()}`, title: "New chat", messages: [newWelcome()], updatedAt: Date.now() };
        setActiveId(fresh.id);
        return [fresh];
      }
      return next;
    });
  };

  const handleSend = (text?: string) => {
    const content = (text ?? query).trim();
    if (!content || isProcessing) return;
    const userMsg: Message = {
      id: messages.length + 1,
      role: "user",
      content,
      timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
    };
    const isFirstUserMsg = !messages.some((m) => m.role === "user");
    updateActive((s) => ({
      ...s,
      title: isFirstUserMsg ? content.slice(0, 40) : s.title,
      messages: [...s.messages, userMsg],
      updatedAt: Date.now(),
    }));
    setQuery("");
    setIsProcessing(true);
    setProcessingStep(0);

    // Cycle through processing steps
    const stepInterval = setInterval(() => {
      setProcessingStep((p) => (p + 1) % processingSteps.length);
    }, 700);

    setTimeout(() => {
      clearInterval(stepInterval);
      setIsProcessing(false);
      setProcessingStep(0);
      const reply = buildReply(content);
      updateActive((s) => ({
        ...s,
        messages: [
          ...s.messages,
          {
            id: s.messages.length + 1,
            role: "assistant",
            content: reply.content,
            timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
            metrics: reply.metrics,
            trace: reply.trace,
          },
        ],
        updatedAt: Date.now(),
      }));
    }, 2800);
  };

  const toggleTrace = (id: number) => setOpenTraces((p) => ({ ...p, [id]: !p[id] }));

  return (
    <DashboardLayout title="Digital Sales Agent" subtitle="Conversational analytics powered by your ecommerce data" breadcrumb="Digital Sales Agent">
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* Chat area */}
        <div className="lg:col-span-9 flex flex-col rounded-xl border border-border bg-gradient-to-b from-card to-card/50 backdrop-blur-md overflow-hidden" style={{ height: "calc(100vh - 200px)" }}>
          <div className="flex items-center gap-2 px-5 py-3 border-b border-border">
            <Sparkles className="w-4 h-4 text-primary" />
            <h3 className="text-sm font-semibold text-foreground">AI Sales Copilot</h3>
            <span className="ml-auto text-xs text-muted-foreground flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-success" /> 3 agents online
            </span>
          </div>

          <div className="flex-1 overflow-auto p-6 space-y-6">
            {messages.map((msg) => (
              <div key={msg.id} className={`flex gap-3 ${msg.role === "user" ? "justify-end" : ""}`}>
                {msg.role === "assistant" && (
                  <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                    <Bot className="w-4 h-4 text-primary" />
                  </div>
                )}
                <div className={`max-w-[85%] ${msg.role === "user" ? "bg-primary/10 border-primary/20" : "bg-secondary/50 border-border"} rounded-xl border p-4`}>
                  <div className="text-sm text-foreground space-y-2 [&_table]:w-full [&_table]:border-collapse [&_table]:my-2 [&_th]:bg-accent [&_th]:text-foreground [&_th]:font-semibold [&_th]:px-3 [&_th]:py-2 [&_th]:text-left [&_th]:border [&_th]:border-border [&_td]:px-3 [&_td]:py-2 [&_td]:border [&_td]:border-border [&_strong]:text-foreground [&_strong]:font-semibold">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content}</ReactMarkdown>
                  </div>

                  {msg.role === "assistant" && msg.trace && (
                    <div className="mt-3 pt-3 border-t border-border/50">
                      <button
                        onClick={() => toggleTrace(msg.id)}
                        className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
                      >
                        {openTraces[msg.id] ? <ChevronDown className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
                        <Workflow className="w-3.5 h-3.5" />
                        Execution trace
                      </button>
                      {openTraces[msg.id] && (
                        <div className="mt-3 space-y-3 rounded-lg bg-background/50 border border-border p-3 text-xs">
                          <div className="flex items-start gap-2">
                            <Target className="w-3.5 h-3.5 text-primary mt-0.5 shrink-0" />
                            <div>
                              <p className="font-medium text-foreground">Intent</p>
                              <p className="text-muted-foreground font-mono">{msg.trace.intent}</p>
                            </div>
                          </div>
                          <div className="flex items-start gap-2">
                            <Filter className="w-3.5 h-3.5 text-primary mt-0.5 shrink-0" />
                            <div className="flex-1">
                              <p className="font-medium text-foreground mb-1">Extracted filters</p>
                              <div className="flex flex-wrap gap-1.5">
                                {msg.trace.filters.map((f) => (
                                  <span key={f.key} className="px-2 py-0.5 rounded bg-accent text-muted-foreground font-mono">
                                    {f.key}: <span className="text-foreground">{f.value}</span>
                                  </span>
                                ))}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-start gap-2">
                            <Database className="w-3.5 h-3.5 text-primary mt-0.5 shrink-0" />
                            <div className="flex-1">
                              <p className="font-medium text-foreground mb-1">Data sources</p>
                              <div className="flex flex-wrap gap-1.5">
                                {msg.trace.sources.map((s) => (
                                  <span key={s} className="px-2 py-0.5 rounded bg-primary/10 text-primary font-mono">{s}</span>
                                ))}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-start gap-2">
                            <Activity className="w-3.5 h-3.5 text-primary mt-0.5 shrink-0" />
                            <div className="flex-1">
                              <p className="font-medium text-foreground mb-1">Steps executed</p>
                              <ol className="space-y-1 list-decimal list-inside text-muted-foreground">
                                {msg.trace.steps.map((step, i) => (
                                  <li key={i}>{step}</li>
                                ))}
                              </ol>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {msg.role === "assistant" && (
                    <div className="flex gap-2 mt-3 pt-3 border-t border-border/50">
                      <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"><Copy className="w-3.5 h-3.5" /></button>
                      <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"><ThumbsUp className="w-3.5 h-3.5" /></button>
                      <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"><ThumbsDown className="w-3.5 h-3.5" /></button>
                      <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"><RefreshCw className="w-3.5 h-3.5" /></button>
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground mt-2">{msg.timestamp}</p>
                </div>
                {msg.role === "user" && (
                  <div className="w-8 h-8 rounded-lg bg-secondary flex items-center justify-center shrink-0">
                    <User className="w-4 h-4 text-foreground" />
                  </div>
                )}
              </div>
            ))}

            {isProcessing && (
              <div className="flex gap-3">
                <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                  <Bot className="w-4 h-4 text-primary animate-pulse" />
                </div>
                <div className="bg-secondary/50 border border-border rounded-xl p-4 max-w-[85%]">
                  <div className="flex items-center gap-3">
                    <div className="flex gap-1">
                      <span className="w-1.5 h-1.5 rounded-full bg-primary animate-bounce" style={{ animationDelay: "0ms" }} />
                      <span className="w-1.5 h-1.5 rounded-full bg-primary animate-bounce" style={{ animationDelay: "150ms" }} />
                      <span className="w-1.5 h-1.5 rounded-full bg-primary animate-bounce" style={{ animationDelay: "300ms" }} />
                    </div>
                    <p className="text-sm text-muted-foreground">{processingSteps[processingStep]}</p>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Suggested prompts */}
          <div className="px-4 pt-3 border-t border-border">
            <div className="flex flex-wrap gap-2">
              {suggestedPrompts.map((p) => (
                <button
                  key={p.label}
                  onClick={() => handleSend(p.label)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border border-border bg-secondary/50 text-secondary-foreground hover:bg-primary/10 hover:border-primary/30 hover:text-primary transition-all"
                >
                  <p.icon className="w-3.5 h-3.5" />
                  {p.label}
                </button>
              ))}
            </div>
          </div>

          {/* Input */}
          <div className="p-4">
            <div className="relative">
              <textarea
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); } }}
                placeholder="Ask about orders, revenue, customers, products, or performance…"
                className="w-full h-20 bg-background/50 border border-border rounded-lg px-4 py-3 pr-14 text-sm text-foreground placeholder:text-muted-foreground resize-none focus:outline-none focus:ring-2 focus:ring-primary/50 transition-all"
              />
              <button
                onClick={() => handleSend()}
                disabled={isProcessing}
                className="absolute bottom-3 right-3 p-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Send className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>

        {/* Insight panel */}
        <aside className="lg:col-span-3 space-y-4 overflow-auto" style={{ maxHeight: "calc(100vh - 200px)" }}>
          <div className="rounded-xl border border-border bg-card backdrop-blur-md p-4">
            <div className="flex items-center gap-2 mb-3">
              <BarChart3 className="w-4 h-4 text-primary" />
              <h3 className="text-sm font-semibold text-foreground">Key Metrics</h3>
            </div>
            {lastAssistantWithMetrics?.metrics ? (
              <div className="grid grid-cols-2 gap-2">
                {lastAssistantWithMetrics.metrics.map((m) => (
                  <div key={m.label} className="rounded-lg border border-border bg-background/50 p-3">
                    <p className="text-xs text-muted-foreground truncate">{m.label}</p>
                    <p className="text-sm font-bold text-foreground mt-1">{m.value}</p>
                    {m.change && (
                      <p className={`text-xs mt-0.5 ${m.positive ? "text-success" : "text-destructive"}`}>{m.change}</p>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-xs text-muted-foreground">Ask a question to see metrics here.</p>
            )}
          </div>

          <div className="rounded-xl border border-border bg-card backdrop-blur-md p-4">
            <div className="flex items-center gap-2 mb-3">
              <DollarSign className="w-4 h-4 text-primary" />
              <h3 className="text-sm font-semibold text-foreground">Category Breakdown</h3>
            </div>
            <div className="space-y-2">
              {[
                { name: "Electronics", pct: 62, value: "$279K" },
                { name: "Accessories", pct: 21, value: "$94K" },
                { name: "Apparel", pct: 11, value: "$50K" },
                { name: "Home", pct: 6, value: "$28K" },
              ].map((c) => (
                <div key={c.name}>
                  <div className="flex justify-between text-xs mb-1">
                    <span className="text-foreground">{c.name}</span>
                    <span className="text-muted-foreground">{c.value}</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-accent overflow-hidden">
                    <div className="h-full bg-primary rounded-full" style={{ width: `${c.pct}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-xl border border-border bg-card backdrop-blur-md p-4">
            <div className="flex items-center gap-2 mb-3">
              <Activity className="w-4 h-4 text-primary" />
              <h3 className="text-sm font-semibold text-foreground">Comparison Insights</h3>
            </div>
            <div className="space-y-3 text-xs">
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">vs Last Month</span>
                <span className="text-success font-medium">+12.5%</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">vs Last Quarter</span>
                <span className="text-success font-medium">+34.2%</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-muted-foreground">vs YoY</span>
                <span className="text-success font-medium">+58.7%</span>
              </div>
              <div className="flex justify-between items-center pt-2 border-t border-border">
                <span className="text-muted-foreground">Forecast Q2</span>
                <span className="text-foreground font-medium">$1.45M</span>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </DashboardLayout>
  );
}
