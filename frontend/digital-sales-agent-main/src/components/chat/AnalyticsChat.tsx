import { useEffect, useState, type ReactNode } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  Send, Bot, User,
  Sparkles, ChevronDown, ChevronRight,
  BarChart3, Anchor, Ship, MapPin,
  DollarSign, Activity, Database, Workflow,
  CheckCircle2, Brain, Route, Maximize2, Minimize2,
} from "lucide-react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";

const createRequestId = () => {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `req_${Date.now()}_${Math.random().toString(36).slice(2)}`;
};

export interface InsightMetric {
  label: string;
  value: string;
  change?: string;
  positive?: boolean;
}

export interface ExecutionTrace {
  intent?: string;
  filters?: { key: string; value: string }[];
  sources?: string[];
  steps?: string[];
  raw?: unknown[];
  displayIntent?: string;
  route?: string;
  likelyPath?: string;
  phases?: string[];
  agents?: string[];
  sqlGenerated?: boolean;
  totalTokens?: number;
  visibleFilters?: { key: string; value: string }[];
  hiddenFilterCount?: number;
  stages?: { title: string; detail: string; area: string; status: string }[];
}

export interface Message {
  id: number;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
  metrics?: InsightMetric[];
  trace?: ExecutionTrace;
}

const processingSteps = [
  "Understanding your query…",
  "Detecting intent & voyage context…",
  "Querying voyage & ops data…",
  "Aggregating results…",
  "Generating insights…",
];

/** Short one-tap prompts — concrete voyage / vessel / port examples (avoid vague “about ports” asks). */
const suggestedPrompts = [
  { icon: Route, label: "Voyage 2302 — delays & ports" },
  { icon: Anchor, label: "Stena Conquest — recent voyages" },
  { icon: MapPin, label: "Singapore — voyages calling this port" },
  { icon: Ship, label: "Top 5 voyages by PnL" },
];

const newWelcome = (text: string): Message => ({
  id: 1,
  role: "assistant",
  content: text,
  timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
});

const getTraceValue = (entry: unknown, key: string): unknown => {
  return entry && typeof entry === "object" ? (entry as Record<string, unknown>)[key] : undefined;
};

const humanizeToken = (value: string) => value
  .replace(/[_-]+/g, " ")
  .replace(/\b\w/g, (char) => char.toUpperCase());

const humanizeIntent = (intent?: string) => {
  if (!intent) return "Analytical request";
  const parts = intent.split(".");
  return humanizeToken(parts[parts.length - 1] || intent);
};

const displaySource = (source: string) => {
  const normalized = source.toLowerCase();
  if (normalized.includes("finance")) return "Finance Agent";
  if (normalized.includes("ops")) return "Ops Agent";
  if (normalized.includes("mongo")) return "Mongo Agent";
  if (normalized.includes("llm")) return "LLM Reasoning";
  if (normalized.includes("postgres")) return "Postgres";
  if (normalized.includes("redis")) return "Session Memory";
  return humanizeToken(source);
};

const visibleFilter = ({ key, value }: { key: string; value: string }) => {
  if (key === "voyage_ids") {
    const count = value.split(",").filter(Boolean).length;
    return { key: "voyage set", value: `${count} matched voyages` };
  }
  if (value.length > 70) return { key: humanizeToken(key), value: `${value.slice(0, 67)}...` };
  return { key: humanizeToken(key), value };
};

const buildTraceStages = (steps: string[], sources: string[]) => {
  const stepText = steps.join(" ").toLowerCase();
  const hasComposite = stepText.includes("composite");
  const hasFinance = sources.some((s) => s.toLowerCase().includes("finance"));
  const hasOps = sources.some((s) => s.toLowerCase().includes("ops"));
  const hasLlm = sources.some((s) => s.toLowerCase().includes("llm")) || stepText.includes("token");

  return [
    {
      title: "Understood request",
      detail: "Detected intent, extracted filters, and prepared the session context.",
      area: "Intent",
      status: "Done",
    },
    {
      title: hasComposite ? "Planned multi-agent route" : "Planned execution route",
      detail: hasComposite ? "Selected a composite workflow across multiple data agents." : "Selected the best available route for the query.",
      area: "Planner",
      status: "Done",
    },
    {
      title: "Queried analytical data",
      detail: [
        hasFinance ? "Finance metrics" : "",
        hasOps ? "operations context" : "",
      ].filter(Boolean).join(" + ") || "Backend data sources",
      area: hasFinance && hasOps ? "Finance + Ops" : hasFinance ? "Finance" : hasOps ? "Ops" : "Data",
      status: "Done",
    },
    {
      title: "Generated admin response",
      detail: hasLlm ? "Summarized retrieved evidence with the LLM layer." : "Prepared the final response payload.",
      area: "Response",
      status: "Done",
    },
  ];
};

const normalizeTrace = (data: unknown): ExecutionTrace | undefined => {
  if (!data || typeof data !== "object") return undefined;
  const payload = data as Record<string, unknown>;
  const rawTrace = Array.isArray(payload.trace) ? payload.trace : [];
  const slots = payload.slots && typeof payload.slots === "object" ? payload.slots as Record<string, unknown> : {};
  const sources = new Set<string>();
  const phases = new Set<string>();
  const steps: string[] = [];
  let likelyPath: string | undefined;
  let sqlGenerated = Boolean(payload.dynamic_sql_used);
  let totalTokens = 0;

  rawTrace.forEach((entry, index) => {
    const node = getTraceValue(entry, "node") ?? getTraceValue(entry, "step") ?? getTraceValue(entry, "event");
    const status = getTraceValue(entry, "status") ?? getTraceValue(entry, "phase");
    const phase = getTraceValue(entry, "phase");
    const path = getTraceValue(entry, "likely_path");
    const source = getTraceValue(entry, "source") ?? getTraceValue(entry, "agent");
    const tokenEstimate = getTraceValue(entry, "total_tokens_est");
    const sqlPresent = getTraceValue(entry, "sql_present");
    const sql = getTraceValue(entry, "sql");
    if (typeof phase === "string" && phase.trim()) phases.add(phase.trim());
    if (typeof path === "string" && path.trim()) likelyPath = path.trim();
    if (typeof source === "string" && source.trim()) sources.add(source.trim());
    if (sqlPresent === true || (typeof sql === "string" && sql.trim())) sqlGenerated = true;
    if (typeof tokenEstimate === "number" && Number.isFinite(tokenEstimate)) {
      totalTokens = Math.max(totalTokens, tokenEstimate);
    }
    const label = [node, status].filter(Boolean).join(" - ");
    steps.push(label || `Trace event ${index + 1}`);
  });

  const filters = Object.entries(slots)
    .filter(([, value]) => value !== null && value !== undefined && value !== "" && value !== false)
    .map(([key, value]) => ({ key, value: String(value) }));

  const intent = typeof payload.intent_key === "string" ? payload.intent_key : undefined;
  const dynamicSqlAgents = Array.isArray(payload.dynamic_sql_agents)
    ? payload.dynamic_sql_agents.filter((agent): agent is string => typeof agent === "string")
    : [];
  dynamicSqlAgents.forEach((agent) => sources.add(agent));
  const sourceList = Array.from(sources);
  const displaySources = sourceList.map(displaySource);
  const visibleFilters = filters
    .filter(({ value }) => value.length <= 160 || value.includes(","))
    .map(visibleFilter)
    .slice(0, 5);

  return {
    intent,
    displayIntent: humanizeIntent(intent),
    route: steps.some((step) => step.toLowerCase().includes("composite")) ? "Composite workflow" : "Single route",
    likelyPath: likelyPath ? humanizeToken(likelyPath) : undefined,
    phases: Array.from(phases).map(humanizeToken),
    agents: Array.from(sources).map(displaySource),
    sqlGenerated,
    totalTokens: totalTokens || undefined,
    filters,
    visibleFilters,
    hiddenFilterCount: Math.max(0, filters.length - visibleFilters.length),
    sources: displaySources,
    steps,
    stages: buildTraceStages(steps, sourceList),
    raw: rawTrace,
  };
};

interface AnalyticsChatProps {
  title?: string;
  welcome?: string;
  seed?: Message[];
  showInsightPanel?: boolean;
  showExecutionTrace?: boolean;
  height?: string;
}

export default function AnalyticsChat({
  title = "AI Sales Copilot",
  welcome = "Welcome to the AI Sales Copilot. Ask about voyages, vessels, ports, delays, cargo, or financial KPIs — I'll route your question to the right data agent.",
  seed,
  showInsightPanel = true,
  showExecutionTrace = false,
  height = "calc(100vh - 220px)",
}: AnalyticsChatProps) {
  const [messages, setMessages] = useState<Message[]>(seed ?? [newWelcome(welcome)]);
  const [query, setQuery] = useState("");
  const [openTraces, setOpenTraces] = useState<Record<number, boolean>>({});
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStep, setProcessingStep] = useState(0);
  /** In-tab fullscreen overlay (entire viewport); Esc exits. Not Element.requestFullscreen. */
  const [chatMaximized, setChatMaximized] = useState(false);

  useEffect(() => {
    if (!chatMaximized) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setChatMaximized(false);
    };
    window.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [chatMaximized]);

  const getSession = () => {
    if (window.__dsa_session?.session_id) return window.__dsa_session.session_id;
    try {
      const raw = sessionStorage.getItem("dsa_session");
      if (raw) {
        const parsed = JSON.parse(raw);
        window.__dsa_session = parsed;
        return parsed.session_id;
      }
    } catch {}
    return `session_${Date.now()}_${Math.random().toString(36).slice(2)}`;
  };

  const effectiveSessionId = getSession();

  const lastAssistantWithMetrics = [...messages].reverse().find((m) => m.role === "assistant" && m.metrics);

  const handleSend = async (text?: string) => {
    const content = (text ?? query).trim();
    if (!content || isProcessing) return;
    const requestId = createRequestId();
    const userMsg: Message = {
      id: messages.length + 1,
      role: "user",
      content,
      timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
    };
    setMessages((prev) => [...prev, userMsg]);
    setQuery("");
    setIsProcessing(true);
    setProcessingStep(0);

    const stepInterval = setInterval(() => {
      setProcessingStep((p) => (p + 1) % processingSteps.length);
    }, 700);

    try {
      const res = await fetch("http://localhost:8010/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: content,
          session_id: effectiveSessionId,
          request_id: requestId,
          chat_history: messages
            .filter(m => m.role === "user" || m.role === "assistant")
            .map(m => ({ role: m.role, content: m.content }))
        }),
      });
      const data = await res.json();

      clearInterval(stepInterval);
      setIsProcessing(false);
      setProcessingStep(0);

      setMessages((prev) => [
        ...prev,
        {
          id: prev.length + 1,
          role: "assistant",
          content: data.clarification || data.answer || data.response || data.result || JSON.stringify(data),
          timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
          trace: showExecutionTrace ? normalizeTrace(data) : undefined,
        },
      ]);
    } catch (err) {
      clearInterval(stepInterval);
      setIsProcessing(false);
      setProcessingStep(0);

      setMessages((prev) => [
        ...prev,
        {
          id: prev.length + 1,
          role: "assistant",
          content: "Sorry, could not reach the backend. Please make sure the server is running at http://localhost:8010",
          timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        },
      ]);
    }
  };

  const toggleTrace = (id: number) => setOpenTraces((p) => ({ ...p, [id]: !p[id] }));

  /** Full viewport within the browser tab (fixed inset-0); Esc exits. Not OS / requestFullscreen. */
  const maximizedShell = (node: ReactNode) =>
    chatMaximized ? (
      <div className="fixed inset-0 z-[100] flex flex-col bg-background">
        <div className="flex flex-col flex-1 min-h-0 w-full h-full overflow-hidden">{node}</div>
      </div>
    ) : (
      <div className="w-full">{node}</div>
    );

  const chatPanel = (
    <div
      className={`flex flex-col border border-border bg-gradient-to-b from-card to-card/50 backdrop-blur-md overflow-hidden ${chatMaximized ? "flex-1 min-h-0 h-full rounded-none border-0 shadow-none" : "rounded-xl shadow-sm"}`}
      style={{ height: chatMaximized ? "100%" : height }}
    >
      <div className="flex items-center gap-2 px-5 py-3 border-b border-border shrink-0">
        <Sparkles className="w-4 h-4 text-primary shrink-0" />
        <h3 className="text-sm font-semibold text-foreground truncate min-w-0 flex-1">{title}</h3>
        <span className="text-xs text-muted-foreground hidden sm:flex items-center gap-1.5 shrink-0">
          <span className="w-1.5 h-1.5 rounded-full bg-[hsl(var(--success))]" /> 3 agents online
        </span>
        <button
          type="button"
          onClick={() => setChatMaximized((v) => !v)}
          className="shrink-0 p-1.5 rounded-md border border-border/80 bg-background/60 text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
          aria-expanded={chatMaximized}
          aria-label={chatMaximized ? "Exit fullscreen" : "Fullscreen chat"}
          title={chatMaximized ? "Exit fullscreen (Esc)" : "Fullscreen chat (this tab)"}
        >
          {chatMaximized ? <Minimize2 className="w-4 h-4" /> : <Maximize2 className="w-4 h-4" />}
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-auto p-6 space-y-6">
        {messages.map((msg) => (
          <div key={msg.id} className={`flex gap-3 ${msg.role === "user" ? "justify-end" : ""}`}>
            {msg.role === "assistant" && (
              <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                <Bot className="w-4 h-4 text-primary" />
              </div>
            )}
            <div className={`min-w-0 max-w-[85%] ${msg.role === "user" ? "bg-primary/10 border-primary/20" : "bg-secondary/50 border-border"} rounded-xl border p-4`}>
              <div
                className="overflow-x-auto w-full text-sm text-foreground space-y-2
                  [&_strong]:text-foreground [&_strong]:font-semibold
                  [&_table]:border-collapse [&_table]:text-xs [&_table]:w-full
                  [&_thead]:sticky [&_thead]:top-0 [&_thead]:z-10
                  [&_th]:bg-accent [&_th]:text-foreground [&_th]:font-semibold
                  [&_th]:px-3 [&_th]:py-2 [&_th]:text-left [&_th]:align-top
                  [&_th]:border-b [&_th]:border-border
                  [&_th]:break-words [&_th]:whitespace-normal
                  [&_td]:px-3 [&_td]:py-2 [&_td]:align-top
                  [&_td]:border-b [&_td]:border-border/60
                  [&_td]:break-words [&_td]:whitespace-normal
                  [&_tbody_tr:last-child_td]:border-b-0
                  [&_tbody_tr:hover]:bg-muted/40"
              >
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={{
                    table: ({ node: _n, ...props }) => (
                      <div className="overflow-x-auto my-3 rounded-lg border border-border">
                        <table className="w-full border-collapse text-sm" {...props} />
                      </div>
                    ),
                    th: ({ node: _n, ...props }) => (
                      <th className="px-3 py-2 text-left border-b border-border bg-accent font-semibold text-foreground whitespace-nowrap" {...props} />
                    ),
                    td: ({ node: _n, ...props }) => (
                      <td className="px-3 py-2 border-b border-border/60 text-foreground" {...props} />
                    ),
                  }}
                >
                  {msg.content}
                </ReactMarkdown>
              </div>

              {showExecutionTrace && msg.role === "assistant" && msg.trace && (
                <div className="mt-3 pt-3 border-t border-border/50">
                  <button
                    onClick={() => toggleTrace(msg.id)}
                    className="w-full flex items-center gap-2 rounded-lg border border-border bg-background/70 px-3 py-1.5 text-left hover:bg-accent/40 transition-colors"
                  >
                    <div className="min-w-0 flex-1">
                      <span className="text-xs font-semibold text-foreground">Execution trace</span>
                      <span className="ml-2 text-[10px] text-muted-foreground">Admin diagnostics</span>
                    </div>
                    {openTraces[msg.id] ? <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />}
                  </button>
                  {openTraces[msg.id] && (
                    <div className="mt-2 rounded-xl border border-border bg-gradient-to-br from-background via-background to-primary/5 shadow-sm overflow-hidden text-xs">
                      <div className="px-3 py-2 border-b border-border bg-muted/20">
                        <div className="flex items-center justify-between gap-3">
                          <div>
                            <p className="text-xs font-semibold text-foreground">Run diagnostics</p>
                            <p className="text-[10px] text-muted-foreground">Phase, path, agent, SQL, and token summary</p>
                          </div>
                          <span className="inline-flex items-center gap-1.5 px-2 py-1 rounded-full bg-[hsl(var(--success))]/10 text-[hsl(var(--success))] font-semibold">
                            <CheckCircle2 className="w-3 h-3" />
                            Complete
                          </span>
                        </div>
                      </div>
                      <div className="divide-y divide-border">
                        <div className="grid grid-cols-[150px_1fr] gap-3 px-3 py-2.5 hover:bg-muted/20">
                          <div className="flex items-center gap-2 text-muted-foreground font-medium">
                            <Activity className="w-3.5 h-3.5 text-primary" />
                            Phase
                          </div>
                          <div className="flex flex-wrap gap-1.5">
                            {(msg.trace.phases?.length ? msg.trace.phases : ["Not provided"]).slice(0, 4).map((phase) => (
                              <span key={phase} className="px-2 py-0.5 rounded-full bg-accent text-foreground font-medium">{phase}</span>
                            ))}
                          </div>
                        </div>
                        <div className="grid grid-cols-[150px_1fr] gap-3 px-3 py-2.5 hover:bg-muted/20">
                          <div className="flex items-center gap-2 text-muted-foreground font-medium">
                            <Route className="w-3.5 h-3.5 text-primary" />
                            Likely path
                          </div>
                          <span className="text-foreground font-semibold">{msg.trace.likelyPath || msg.trace.route || "Not provided"}</span>
                        </div>
                        <div className="grid grid-cols-[150px_1fr] gap-3 px-3 py-2.5 hover:bg-muted/20">
                          <div className="flex items-center gap-2 text-muted-foreground font-medium">
                            <Brain className="w-3.5 h-3.5 text-primary" />
                            Agent used
                          </div>
                          <span className="text-foreground font-semibold break-words">
                            {(msg.trace.agents?.length ? msg.trace.agents : ["Not provided"]).join(", ")}
                          </span>
                        </div>
                        <div className="grid grid-cols-[150px_1fr] gap-3 px-3 py-2.5 hover:bg-muted/20">
                          <div className="flex items-center gap-2 text-muted-foreground font-medium">
                            <Database className="w-3.5 h-3.5 text-primary" />
                            SQL generated
                          </div>
                          <span className={`w-fit inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full font-semibold ${msg.trace.sqlGenerated ? "bg-[hsl(var(--success))]/10 text-[hsl(var(--success))]" : "bg-muted text-muted-foreground"}`}>
                            <CheckCircle2 className="w-3 h-3" />
                            {msg.trace.sqlGenerated ? "Yes" : "No"}
                          </span>
                        </div>
                        <div className="grid grid-cols-[150px_1fr] gap-3 px-3 py-2.5 hover:bg-muted/20">
                          <div className="flex items-center gap-2 text-muted-foreground font-medium">
                            <Workflow className="w-3.5 h-3.5 text-primary" />
                            Total tokens
                          </div>
                          <span className="text-foreground font-semibold">
                            {typeof msg.trace.totalTokens === "number" ? msg.trace.totalTokens.toLocaleString() : "Not available"}
                          </span>
                        </div>
                      </div>
                    </div>
                  )}
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

      <div className="px-4 pt-3 border-t border-border shrink-0">
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

      <div className="p-4 shrink-0">
        <div className="relative">
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(); } }}
            placeholder="Ask about voyages, vessels, ports, delays, cargo, or PnL…"
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
  );

  const metricsPanel = (
    <div className="space-y-4">
      <div className="rounded-xl border border-border bg-card backdrop-blur-md p-4">
        <div className="flex items-center gap-2 mb-3">
          <BarChart3 className="w-4 h-4 text-primary" />
          <h3 className="text-sm font-semibold text-foreground">Key Metrics</h3>
        </div>
        {lastAssistantWithMetrics?.metrics ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {lastAssistantWithMetrics.metrics.map((m) => (
              <div key={m.label} className="rounded-lg border border-border bg-background/50 p-3">
                <p className="text-xs text-muted-foreground truncate">{m.label}</p>
                <p className="text-base font-bold text-foreground mt-1">{m.value}</p>
                {m.change && (
                  <p className={`text-xs mt-0.5 ${m.positive ? "text-[hsl(var(--success))]" : "text-destructive"}`}>{m.change}</p>
                )}
              </div>
            ))}
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">Ask a question in the Chat tab to see metrics here.</p>
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
    </div>
  );

  if (!showInsightPanel) {
    return maximizedShell(chatPanel);
  }

  return maximizedShell(
    <Tabs
      defaultValue="chat"
      className={`w-full ${chatMaximized ? "flex flex-col flex-1 min-h-0 h-full" : ""}`}
    >
      <TabsList className={chatMaximized ? "shrink-0" : ""}>
        <TabsTrigger value="chat">Chat</TabsTrigger>
        <TabsTrigger value="metrics">Metrics</TabsTrigger>
      </TabsList>
      <TabsContent
        value="chat"
        className={`mt-4 ${chatMaximized ? "flex-1 min-h-0 flex flex-col" : ""}`}
      >
        {chatPanel}
      </TabsContent>
      <TabsContent
        value="metrics"
        className={`mt-4 ${chatMaximized ? "flex-1 min-h-0 overflow-auto" : ""}`}
      >
        {metricsPanel}
      </TabsContent>
    </Tabs>,
  );
}