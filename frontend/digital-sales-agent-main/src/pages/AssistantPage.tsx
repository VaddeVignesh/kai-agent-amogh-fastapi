import DashboardLayout from "@/components/dashboard/DashboardLayout";
import AnalyticsChat, { type Message } from "@/components/chat/AnalyticsChat";

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
  },
];

export default function AssistantPage() {
  return (
    <DashboardLayout
      title="Digital Sales Agent"
      subtitle="Conversational analytics powered by your ecommerce data"
      breadcrumb="Digital Sales Agent"
    >
      <AnalyticsChat seed={seedMessages} showExecutionTrace={false} />
    </DashboardLayout>
  );
}
