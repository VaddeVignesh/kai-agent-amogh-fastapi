import { useState, FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { Sparkles, Mail, Lock, Loader2, Sun, Moon, ShieldCheck } from "lucide-react";
import { useTheme } from "@/hooks/use-theme";
import { toast } from "@/hooks/use-toast";

declare global {
  interface Window {
    __dsa_session?: { role: string; session_id: string };
  }
}

export default function LoginPage() {
  const navigate = useNavigate();
  const { theme, toggle } = useTheme();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!email || !password) {
      toast({ title: "Missing fields", description: "Email and password are required.", variant: "destructive" });
      return;
    }
    setLoading(true);
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 10000);
    try {
      const res = await fetch("http://localhost:8010/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: email, password: password }),
        signal: controller.signal,
      });
      const data = await res.json();
      if (!data.success) {
        toast({ title: "Login failed", description: data.message, variant: "destructive" });
        setLoading(false);
        return;
      }
      window.__dsa_session = { role: data.role, session_id: data.session_id };
      sessionStorage.setItem("dsa_session", JSON.stringify({ role: data.role, session_id: data.session_id }));
      toast({ title: "Welcome!", description: `Logged in as ${data.role}` });
      navigate(data.role === "admin" ? "/admin" : "/assistant", { replace: true });
    } catch (err) {
      const timedOut = err instanceof DOMException && err.name === "AbortError";
      toast({
        title: timedOut ? "Login timed out" : "Error",
        description: timedOut ? "Backend did not respond within 10 seconds. Please retry." : "Could not reach backend.",
        variant: "destructive",
      });
    } finally {
      window.clearTimeout(timeout);
      setLoading(false);
    }
  };

  const isAdmin = false;

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="h-14 border-b border-border bg-card/50 backdrop-blur-sm flex items-center justify-between px-6">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-primary-foreground" />
          </div>
          <span className="text-sm font-semibold text-foreground">Digital Sales Agent</span>
        </div>
        <button
          onClick={toggle}
          className="p-2 rounded-lg hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"
          aria-label="Toggle theme"
        >
          {theme === "dark" ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
        </button>
      </header>

      <main className="flex-1 flex items-center justify-center p-6 relative overflow-hidden">
        <div className="absolute inset-0 -z-10 bg-[radial-gradient(circle_at_30%_20%,hsl(var(--primary)/0.12),transparent_50%),radial-gradient(circle_at_70%_80%,hsl(var(--primary)/0.08),transparent_50%)]" />

        <div className="w-full max-w-md">
          <div className="text-center mb-6">
            <div className="inline-flex w-12 h-12 rounded-2xl bg-primary items-center justify-center mb-4 shadow-lg shadow-primary/20">
              {isAdmin ? (
                <ShieldCheck className="w-6 h-6 text-primary-foreground" />
              ) : (
                <Sparkles className="w-6 h-6 text-primary-foreground" />
              )}
            </div>
            <h1 className="text-2xl font-bold text-foreground mb-1">Welcome back</h1>
            <p className="text-sm text-muted-foreground">Sign in to access your Digital Sales Agent</p>
          </div>

          <div className="rounded-xl border border-border bg-card shadow-sm p-6">
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="space-y-1.5">
                <label htmlFor="email" className="text-xs font-medium text-foreground">Username</label>
                <div className="relative">
                  <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                  <input
                    id="email"
                    type="text"
                    autoComplete="username"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="admin or customer"
                    className="w-full h-10 pl-9 pr-3 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </div>
              </div>

              <div className="space-y-1.5">
                <label htmlFor="password" className="text-xs font-medium text-foreground">Password</label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                  <input
                    id="password"
                    type="password"
                    autoComplete="current-password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className="w-full h-10 pl-9 pr-3 rounded-lg border border-input bg-background text-sm text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full h-10 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors flex items-center justify-center gap-2 disabled:opacity-60"
              >
                {loading ? (
                  <><Loader2 className="w-4 h-4 animate-spin" />Signing in...</>
                ) : (
                  <>Sign in</>
                )}
              </button>
            </form>
          </div>

          <p className="text-center text-xs text-muted-foreground mt-6">
            Use <strong>admin</strong> / <strong>admin123</strong> or <strong>customer</strong> / <strong>cust123</strong>
          </p>
        </div>
      </main>
    </div>
  );
}