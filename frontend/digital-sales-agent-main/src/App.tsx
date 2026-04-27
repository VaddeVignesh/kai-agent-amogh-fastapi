import { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ThemeProvider } from "@/hooks/use-theme";
import AssistantPage from "./pages/AssistantPage";
import AdminPage from "./pages/AdminPage";
import LoginPage from "./pages/LoginPage";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient();

type Auth = { role: string; session_id: string } | null;

const getAuth = (): Auth => {
  if (window.__dsa_session) return window.__dsa_session as Auth;
  try {
    const raw = sessionStorage.getItem("dsa_session");
    if (raw) {
      const parsed = JSON.parse(raw) as Auth;
      window.__dsa_session = parsed ?? undefined;
      return parsed;
    }
  } catch {}
  return null;
};

const RequireAuth = ({ role, children }: { role?: "user" | "admin" | "customer"; children: ReactNode }) => {
  const auth = getAuth();
  if (!auth) return <Navigate to="/login" replace />;
  if (role) {
    const allowedRoles: Record<string, string[]> = {
      user: ["user", "customer"],
      customer: ["user", "customer"],
      admin: ["admin"],
    };
    const allowed = allowedRoles[role] ?? [role];
    if (!allowed.includes(auth.role)) {
      return <Navigate to={auth.role === "admin" ? "/admin" : "/assistant"} replace />;
    }
  }
  return <>{children}</>;
};

const App = () => (
  <QueryClientProvider client={queryClient}>
    <ThemeProvider>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<Navigate to="/login" replace />} />
            <Route path="/login" element={<LoginPage />} />
            <Route
              path="/assistant"
              element={
                <RequireAuth role="customer">
                  <AssistantPage />
                </RequireAuth>
              }
            />
            <Route
              path="/admin"
              element={
                <RequireAuth role="admin">
                  <AdminPage />
                </RequireAuth>
              }
            />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </BrowserRouter>
      </TooltipProvider>
    </ThemeProvider>
  </QueryClientProvider>
);

export default App;