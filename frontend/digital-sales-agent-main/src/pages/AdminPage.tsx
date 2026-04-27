import { useMemo } from "react";
import DashboardLayout from "@/components/dashboard/DashboardLayout";
import AnalyticsChat from "@/components/chat/AnalyticsChat";
import {
  Users, Activity, MessageSquare, ShieldCheck, TrendingUp, Server, Sparkles,
  CheckCircle2, AlertTriangle, Database, Cpu, Search, MoreHorizontal,
} from "lucide-react";

interface KPI {
  label: string;
  value: string;
  change: string;
  positive: boolean;
  icon: React.ElementType;
}

interface UserRow {
  name: string;
  email: string;
  role: "Admin" | "Analyst" | "Viewer";
  status: "Active" | "Invited" | "Suspended";
  lastActive: string;
  queries: number;
}

interface SystemRow {
  name: string;
  status: "Operational" | "Degraded" | "Down";
  latency: string;
  uptime: string;
}

export default function AdminPage() {
  const kpis: KPI[] = [
    { label: "Total users", value: "1,284", change: "+12.4%", positive: true, icon: Users },
    { label: "Active sessions", value: "342", change: "+5.1%", positive: true, icon: Activity },
    { label: "Queries today", value: "8,921", change: "+18.7%", positive: true, icon: MessageSquare },
    { label: "Avg response", value: "1.42s", change: "-0.18s", positive: true, icon: TrendingUp },
  ];

  const users: UserRow[] = useMemo(() => [
    { name: "Sarah Chen", email: "sarah.chen@company.com", role: "Admin", status: "Active", lastActive: "2m ago", queries: 142 },
    { name: "Marcus Rivera", email: "m.rivera@company.com", role: "Analyst", status: "Active", lastActive: "14m ago", queries: 318 },
    { name: "Priya Patel", email: "priya.p@company.com", role: "Analyst", status: "Active", lastActive: "1h ago", queries: 256 },
    { name: "Jonas Weber", email: "jonas.w@company.com", role: "Viewer", status: "Invited", lastActive: "—", queries: 0 },
    { name: "Aiko Tanaka", email: "a.tanaka@company.com", role: "Analyst", status: "Active", lastActive: "3h ago", queries: 91 },
    { name: "Diego Morales", email: "diego.m@company.com", role: "Viewer", status: "Suspended", lastActive: "5d ago", queries: 24 },
  ], []);

  const systems: SystemRow[] = [
    { name: "Orders DB", status: "Operational", latency: "42ms", uptime: "99.99%" },
    { name: "Product Catalog", status: "Operational", latency: "38ms", uptime: "99.98%" },
    { name: "Customer Data", status: "Operational", latency: "51ms", uptime: "99.97%" },
    { name: "Analytics Pipeline", status: "Degraded", latency: "186ms", uptime: "99.42%" },
    { name: "AI Gateway", status: "Operational", latency: "612ms", uptime: "99.95%" },
  ];

  const roleStyles: Record<UserRow["role"], string> = {
    Admin: "bg-primary/10 text-primary",
    Analyst: "bg-success/10 text-[hsl(var(--success))]",
    Viewer: "bg-muted text-muted-foreground",
  };

  const statusStyles: Record<UserRow["status"], string> = {
    Active: "bg-success/10 text-[hsl(var(--success))]",
    Invited: "bg-warning/10 text-[hsl(var(--warning))]",
    Suspended: "bg-destructive/10 text-destructive",
  };

  const sysStatusIcon = (s: SystemRow["status"]) => {
    if (s === "Operational") return <CheckCircle2 className="w-4 h-4 text-[hsl(var(--success))]" />;
    if (s === "Degraded") return <AlertTriangle className="w-4 h-4 text-[hsl(var(--warning))]" />;
    return <AlertTriangle className="w-4 h-4 text-destructive" />;
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
                <span className={`text-xs font-medium ${kpi.positive ? "text-[hsl(var(--success))]" : "text-destructive"}`}>
                  {kpi.change}
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
                className="h-8 pl-8 pr-3 rounded-md border border-input bg-background text-xs w-48 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </div>
            <button className="h-8 px-3 rounded-md bg-primary text-primary-foreground text-xs font-medium hover:bg-primary/90">
              Invite user
            </button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-muted-foreground border-b border-border bg-muted/30">
                <th className="text-left font-medium px-4 py-2.5">User</th>
                <th className="text-left font-medium px-4 py-2.5">Role</th>
                <th className="text-left font-medium px-4 py-2.5">Status</th>
                <th className="text-left font-medium px-4 py-2.5">Last active</th>
                <th className="text-right font-medium px-4 py-2.5">Queries</th>
                <th className="px-4 py-2.5 w-8"></th>
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.email} className="border-b border-border last:border-0 hover:bg-muted/30">
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-full bg-primary/10 text-primary text-xs font-semibold flex items-center justify-center">
                        {u.name.split(" ").map((n) => n[0]).join("")}
                      </div>
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-foreground truncate">{u.name}</div>
                        <div className="text-xs text-muted-foreground truncate">{u.email}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${roleStyles[u.role]}`}>
                      {u.role}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${statusStyles[u.status]}`}>
                      {u.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">{u.lastActive}</td>
                  <td className="px-4 py-3 text-right text-sm text-foreground font-medium">{u.queries.toLocaleString()}</td>
                  <td className="px-4 py-3">
                    <button className="p-1 rounded hover:bg-accent text-muted-foreground hover:text-foreground">
                      <MoreHorizontal className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* System status + activity */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 rounded-xl border border-border bg-card shadow-sm">
          <div className="p-4 border-b border-border">
            <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
              <Server className="w-4 h-4 text-primary" /> System status
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">Live health of connected data sources & services</p>
          </div>
          <div className="divide-y divide-border">
            {systems.map((s) => (
              <div key={s.name} className="flex items-center justify-between px-4 py-3">
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
                <div className="flex items-center gap-6 text-xs">
                  <div className="text-right">
                    <div className="text-muted-foreground">Latency</div>
                    <div className="text-foreground font-medium">{s.latency}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-muted-foreground">Uptime</div>
                    <div className="text-foreground font-medium">{s.uptime}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card shadow-sm">
          <div className="p-4 border-b border-border">
            <h2 className="text-sm font-semibold text-foreground flex items-center gap-2">
              <ShieldCheck className="w-4 h-4 text-primary" /> Audit log
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">Recent admin activity</p>
          </div>
          <ul className="divide-y divide-border">
            {[
              { who: "Sarah Chen", what: "promoted Marcus to Analyst", when: "2m ago" },
              { who: "System", what: "auto-rotated API keys", when: "1h ago" },
              { who: "Sarah Chen", what: "invited jonas.w@company.com", when: "3h ago" },
              { who: "Priya Patel", what: "exported orders report", when: "5h ago" },
              { who: "System", what: "RLS policy review passed", when: "1d ago" },
            ].map((a, i) => (
              <li key={i} className="px-4 py-3 text-xs">
                <div className="text-foreground">
                  <span className="font-medium">{a.who}</span>{" "}
                  <span className="text-muted-foreground">{a.what}</span>
                </div>
                <div className="text-muted-foreground mt-0.5 flex items-center gap-1">
                  <Cpu className="w-3 h-3" /> {a.when}
                </div>
              </li>
            ))}
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
        />
      </div>
    </DashboardLayout>
  );
}
