import DashboardLayout from "@/components/dashboard/DashboardLayout";
import AnalyticsChat, { type Message } from "@/components/chat/AnalyticsChat";

const seedMessages: Message[] = [
  {
    id: 1,
    role: "assistant",
    content:
      "Welcome to the AI Sales Copilot. Ask about voyages, vessels, ports, delays, cargo, or financial KPIs — I'll route your question to the right data agent.",
    timestamp: "10:00 AM",
  },
];

export default function AssistantPage() {
  return (
    <DashboardLayout
      title="Digital Sales Agent"
      subtitle="Conversational analytics for voyage, vessel, and port operations"
      breadcrumb="Digital Sales Agent"
    >
      <AnalyticsChat seed={seedMessages} showExecutionTrace={false} />
    </DashboardLayout>
  );
}
