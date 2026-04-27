import { useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  Send, Bot, User, Copy, ThumbsUp, ThumbsDown, RefreshCw,
  Sparkles, ChevronDown, ChevronRight,
  TrendingUp, ShoppingCart, Users, Package, Clock, BarChart3,
  DollarSign, Activity, Database, Filter, Target, Workflow,
} from "lucide-react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";

export interface InsightMetric {
  label: string;
  value: string;
  change?: string;
  positive?: boolean;
}

export interface ExecutionTrace {
  intent: string;
  filters: { key: string; value: string }[];
  sources: string[];
  steps: string[];
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

const newWelcome = (text: string): Message => ({
  id: 1,
  role: "assistant",
  content: text,
  timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
});

interface AnalyticsChatProps {
  title?: string;
  welcome?: string;
  seed?: Message[];
  showInsightPanel?: boolean;
  height?: string;
}

export default function AnalyticsChat({
  title = "AI Sales Copilot",
  welcome = "Welcome to the AI Sales Copilot. Ask about orders, revenue, customers, products, or performance — I'll route the question to the right data agent.",
  seed,
  showInsightPanel = true,
  height = "calc(100vh - 220px)",
}: AnalyticsChatProps) {
  const [messages, setMessages] = useState<Message[]>(seed ?? [newWelcome(welcome)]);
  const [query, setQuery] = useState("");
  const [openTraces, setOpenTraces] = useState<Record<number, boolean>>({});
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStep, setProcessingStep] = useState(0);

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
          chat_history: messages
            .filter(m => m.role === "user" || m.role === "assistant")
            .map(m => ({ role: m.role, content: m.content }))
        })
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
          content: data.answer ?? data.response ?? data.result ?? JSON.stringify(data),
          timestamp: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
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

  const chatPanel = (
    <div
      className="flex flex-col rounded-xl border border-border bg-gradient-to-b from-card to-card/50 backdrop-blur-md overflow-hidden"
      style={{ height }}
    >
      <div className="flex items-center gap-2 px-5 py-3 border-b border-border">
        <Sparkles className="w-4 h-4 text-primary" />
        <h3 className="text-sm font-semibold text-foreground">{title}</h3>
        <span className="ml-auto text-xs text-muted-foreground flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-[hsl(var(--success))]" /> 3 agents online
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
    return chatPanel;
  }

  return (
    <Tabs defaultValue="chat" className="w-full">
      <TabsList>
        <TabsTrigger value="chat">Chat</TabsTrigger>
        <TabsTrigger value="metrics">Metrics</TabsTrigger>
      </TabsList>
      <TabsContent value="chat" className="mt-4">
        {chatPanel}
      </TabsContent>
      <TabsContent value="metrics" className="mt-4">
        {metricsPanel}
      </TabsContent>
    </Tabs>
  );
}