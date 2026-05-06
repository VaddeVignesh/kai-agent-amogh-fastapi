import { useEffect, useMemo, useState } from "react";
import DashboardLayout from "@/components/dashboard/DashboardLayout";
import AnalyticsChat from "@/components/chat/AnalyticsChat";
import {
  Users, Activity, MessageSquare, ShieldCheck, TrendingUp, Server, Sparkles,
  CheckCircle2, AlertTriangle, Database, Cpu, Search, MoreHorizontal,
} from "lucide-react";

interface KPI {
  label: string;
  value: string;
  icon: React.ElementType;
}

interface AdminMetrics {
  total_users: number;
  active_sessions: number;
  queries_today: number;
  avg_response_time: number;
}

interface UserRow {
  username: string;
  role: "admin" | "customer" | string;
  status: "Active" | "Offline" | string;
  active_sessions: number;
  last_active: string;
  queries_today: number;
}

interface AuditEvent {
  timestamp: number;
  actor: string;
  role: string;
  action: "login" | "logout" | "query" | string;
  status: string;
  session_id?: string;
  query_preview?: string;
  query_length?: number;
  intent_key?: string;
  duration_seconds?: number;
}

interface SystemRow {
  name: string;
  status: "Operational" | "Degraded" | "Down";
  latency_ms: number | null;
  detail: string;
}

export default function AdminPage() {
  const [metrics, setMetrics] = useState<AdminMetrics>({
    total_users: 0,
    active_sessions: 0,
    queries_today: 0,
    avg_response_time: 0,
  });
  const [users, setUsers] = useState<UserRow[]>([]);
  const [auditEvents, setAuditEvents] = useState<AuditEvent[]>([]);
  const [systems, setSystems] = useState<SystemRow[]>([]);
  const [userSearch, setUserSearch] = useState("");

  useEffect(() => {
    const adminHeaders = () => {
      try {
        const raw = sessionStorage.getItem("dsa_session");
        const sessionId = raw ? JSON.parse(raw)?.session_id : "";
        return sessionId ? { "X-Session-Id": String(sessionId) } : {};
      } catch {
        return {};
      }
    };

    const fetchMetrics = async () => {
      try {
        const res = await fetch("http://localhost:8010/admin/metrics", { headers: adminHeaders() });
        if (res.ok) {
          setMetrics(await res.json());
        }
      } catch (err) {
        console.error("Failed to fetch admin metrics", err);
      }
    };
    const fetchUsers = async () => {
      try {
        const res = await fetch("http://localhost:8010/admin/users", { headers: adminHeaders() });
        if (res.ok) {
          setUsers(await res.json());
        }
      } catch (err) {
        console.error("Failed to fetch admin users", err);
      }
    };
    const fetchAuditEvents = async () => {
      try {
        const res = await fetch("http://localhost:8010/admin/audit-log", { headers: adminHeaders() });
        if (res.ok) {
          setAuditEvents(await res.json());
        }
      } catch (err) {
        console.error("Failed to fetch audit log", err);
      }
    };
    const fetchSystemHealth = async () => {
      try {
        const res = await fetch("http://localhost:8010/admin/system-health", { headers: adminHeaders() });
        if (res.ok) {
          setSystems(await res.json());
        }
      } catch (err) {
        console.error("Failed to fetch system health", err);
      }
    };

    fetchMetrics();
    fetchUsers();
    fetchAuditEvents();
    fetchSystemHealth();
    const interval = window.setInterval(() => {
      fetchMetrics();
      fetchUsers();
      fetchAuditEvents();
      fetchSystemHealth();
    }, 30000);
    return () => window.clearInterval(interval);
  }, []);

  const kpis: KPI[] = [
    { label: "Total users", value: metrics.total_users.toLocaleString(), icon: Users },
    { label: "Active sessions", value: metrics.active_sessions.toLocaleString(), icon: Activity },
    { label: "Queries today", value: metrics.queries_today.toLocaleString(), icon: MessageSquare },
    { label: "Avg response", value: `${metrics.avg_response_time.toFixed(2)}s`, icon: TrendingUp },
  ];

  const filteredUsers = useMemo(() => {
    const q = userSearch.trim().toLowerCase();
    if (!q) return users;
    return users.filter((u) =>
      u.username.toLowerCase().includes(q) ||
      String(u.role).toLowerCase().includes(q) ||
      String(u.status).toLowerCase().includes(q)
    );
  }, [userSearch, users]);

  const roleStyles: Record<string, string> = {
    admin: "bg-primary/10 text-primary",
    customer: "bg-success/10 text-[hsl(var(--success))]",
  };

  const statusStyles: Record<string, string> = {
    Active: "bg-success/10 text-[hsl(var(--success))]",
    Offline: "bg-muted text-muted-foreground",
  };

  const sysStatusIcon = (s: SystemRow["status"]) => {
    if (s === "Operational") return <CheckCircle2 className="w-4 h-4 text-[hsl(var(--success))]" />;
    if (s === "Degraded") return <AlertTriangle className="w-4 h-4 text-[hsl(var(--warning))]" />;
    return <AlertTriangle className="w-4 h-4 text-destructive" />;
  };

  const systemLatency = (s: SystemRow) => (
    typeof s.latency_ms === "number" ? `${s.latency_ms.toFixed(1)}ms` : "—"
  );

  const formatRelativeTime = (timestamp: number) => {
    const ageSeconds = Math.max(0, Math.floor((Date.now() - timestamp * 1000) / 1000));
    if (ageSeconds < 60) return "just now";
    const minutes = Math.floor(ageSeconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  };

  const auditTitle = (event: AuditEvent) => {
    if (event.action === "login") return "logged in";
    if (event.action === "logout") return "logged out";
    if (event.action === "query") return "asked a query";
    return event.action;
  };

  const auditMeta = (event: AuditEvent) => {
    const parts = [];
    if (event.intent_key) parts.push(event.intent_key);
    if (event.query_length) parts.push(`${event.query_length} chars`);
    if (typeof event.duration_seconds === "number") parts.push(`${event.duration_seconds.toFixed(2)}s`);
    if (!parts.length && event.session_id) parts.push(`session ${event.session_id.slice(-6)}`);
    return parts.join(" · ");
  };

  return (
    <DashboardLayout title="Admin Console" subtitle="Manage users, monitor usage, and oversee system health">
      {/* KPI cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {kpis.map((kpi) => {
          const Icon = kpi.icon;
          return (
            <div key={kpi.label} className="rounded-xl border border-border bg-card p-4 shadow-sm">
              <div className="flex items-start justify-between mb-3">
                <div className="w-9 h-9 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Icon className="w-4 h-4 text-primary" />
                </div>
                <span className="inline-flex items-center gap-1.5 text-xs font-medium text-[hsl(var(--success))]">
                  <span className="w-1.5 h-1.5 rounded-full bg-[hsl(var(--success))]" />
                  Live
                </span>
              </div>
              <div className="text-2xl font-bold text-foreground">{kpi.value}</div>
              <div className="text-xs text-muted-foreground mt-0.5">{kpi.label}</div>
            </div>
          );
        })}
      </div>

      {/* Users table */}
      <div className="rounded-xl border border-border bg-card shadow-sm overflow-hidden">
        <div className="flex items-center justify-between p-4 border-b border-border">
          <div>
            <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
              <Users className="w-4 h-4 text-primary" /> Users
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">Manage roles and access</p>
          </div>
          <div className="flex items-center gap-2">
            <div className="relative hidden sm:block">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
              <input
                placeholder="Search users..."
                value={userSearch}
                onChange={(e) => setUserSearch(e.target.value)}
                className="h-8 pl-8 pr-3 rounded-md border border-input bg-background text-xs w-48 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </div>
            <span className="hidden sm:inline-flex items-center gap-1.5 text-xs font-medium text-[hsl(var(--success))]">
              <span className="w-1.5 h-1.5 rounded-full bg-[hsl(var(--success))]" />
              Live
            </span>
          </div>
        </div>
        <div className="max-h-[340px] overflow-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-[1] bg-card">
              <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                <th className="text-left font-medium px-4 py-2.5">User</th>
                <th className="text-left font-medium px-4 py-2.5">Role</th>
                <th className="text-left font-medium px-4 py-2.5">Status</th>
                <th className="text-left font-medium px-4 py-2.5">Last active</th>
                <th className="text-right font-medium px-4 py-2.5">Sessions</th>
                <th className="text-right font-medium px-4 py-2.5">Queries today</th>
                <th className="px-4 py-2.5 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {filteredUsers.map((u) => (
                <tr key={`${u.role}-${u.username}`} className="border-b border-border last:border-0 hover:bg-muted/30">
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-full bg-primary/10 text-primary text-xs font-semibold flex items-center justify-center">
                        {u.username.slice(0, 2).toUpperCase()}
                      </div>
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-foreground truncate">{u.username}</div>
                        <div className="text-xs text-muted-foreground truncate">{u.role}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${roleStyles[u.role] || "bg-muted text-muted-foreground"}`}>
                      {u.role}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${statusStyles[u.status] || "bg-muted text-muted-foreground"}`}>
                      {u.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{u.last_active}</td>
                  <td className="px-4 py-3 text-right text-sm text-foreground font-medium">{u.active_sessions.toLocaleString()}</td>
                  <td className="px-4 py-3 text-right text-sm text-foreground font-medium">{u.queries_today.toLocaleString()}</td>
                  <td className="px-4 py-3">
                    <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground">
                      <MoreHorizontal className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
              {filteredUsers.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-sm text-muted-foreground">
                    No users match your search.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* System status + activity */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 items-start">
        <div className="lg:col-span-2 rounded-xl border border-border bg-card shadow-sm overflow-hidden">
          <div className="p-4 border-b border-border">
            <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
              <Server className="w-4 h-4 text-primary" /> System status
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">Live health of backend data services</p>
          </div>
          <div className="divide-y divide-border max-h-[260px] overflow-y-auto">
            {systems.map((s) => (
              <div key={s.name} className="flex items-center justify-between gap-4 px-4 py-3">
                <div className="flex items-center gap-3 min-w-0">
                  <div className="w-8 h-8 rounded-lg bg-muted flex items-center justify-center">
                    <Database className="w-4 h-4 text-muted-foreground" />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-foreground">{s.name}</div>
                    <div className="text-xs text-muted-foreground flex items-center gap-1.5">
                      {sysStatusIcon(s.status)} {s.status}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-4 text-xs shrink-0">
                  <div className="text-right">
                    <div className="text-muted-foreground">Latency</div>
                    <div className="text-foreground font-medium">{systemLatency(s)}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-muted-foreground">Details</div>
                    <div className="text-foreground font-medium max-w-44 truncate" title={s.detail}>{s.detail}</div>
                  </div>
                </div>
              </div>
            ))}
            {systems.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-muted-foreground">
                Loading system health...
              </div>
            )}
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card shadow-sm overflow-hidden">
          <div className="p-4 border-b border-border">
            <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
              <ShieldCheck className="w-4 h-4 text-primary" /> Audit log
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">Recent user activity</p>
          </div>
          <ul className="divide-y divide-border max-h-[260px] overflow-y-auto">
            {auditEvents.map((event, i) => (
              <li key={`${event.timestamp}-${event.actor}-${i}`} className="px-4 py-2.5 text-xs">
                <div className="text-foreground">
                  <span className="font-medium">{event.actor}</span>{" "}
                  <span className="text-muted-foreground">{auditTitle(event)}</span>
                </div>
                {event.query_preview && (
                  <div className="text-muted-foreground mt-1 truncate">"{event.query_preview}"</div>
                )}
                {auditMeta(event) && (
                  <div className="text-muted-foreground mt-1">{auditMeta(event)}</div>
                )}
                <div className="text-muted-foreground mt-0.5 flex items-center gap-1">
                  <Cpu className="w-3 h-3" /> {event.role} · {formatRelativeTime(event.timestamp)}
                </div>
              </li>
            ))}
            {auditEvents.length === 0 && (
              <li className="px-4 py-8 text-center text-xs text-muted-foreground">
                No recent activity yet.
              </li>
            )}
          </ul>
        </div>
      </div>

      {/* Analytics chat */}
      <div>
        <div className="flex items-center gap-2 mb-3">
          <div className="w-8 h-8 rounded-lg bg-primary/10 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h2 className="text-sm font-semibold text-foreground">Analytics Assistant</h2>
            <p className="text-xs text-muted-foreground">Ask the AI for org-wide ecommerce insights</p>
          </div>
        </div>
        <AnalyticsChat
          title="Admin Analytics Copilot"
          welcome="Admin view enabled. Ask anything about orders, revenue, customers, products, inventory, or platform performance — I have access to all tenants."
          height="640px"
          showExecutionTrace
        />
      </div>
    </DashboardLayout>
  );
}
