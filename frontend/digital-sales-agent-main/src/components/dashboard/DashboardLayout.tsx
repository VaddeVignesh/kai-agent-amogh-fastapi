import { ReactNode } from "react";
import { Sun, Moon, Sparkles } from "lucide-react";
import { useTheme } from "@/hooks/use-theme";

interface DashboardLayoutProps {
  children: ReactNode;
  title: string;
  subtitle: string;
  breadcrumb?: string;
}

export default function DashboardLayout({ children, title, subtitle }: DashboardLayoutProps) {
  const { theme, toggle } = useTheme();

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="h-14 border-b border-border bg-card/50 backdrop-blur-sm flex items-center justify-between px-6 sticky top-0 z-10">
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
      <main className="flex-1 p-6 space-y-6 max-w-6xl w-full mx-auto">
        <div>
          <h1 className="text-2xl font-bold text-foreground mb-1">{title}</h1>
          <p className="text-sm text-muted-foreground">{subtitle}</p>
        </div>
        {children}
      </main>
    </div>
  );
}
