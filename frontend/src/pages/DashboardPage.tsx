import Card from "../components/Card";

export default function DashboardPage() {
  return (
    <Card>
      <h1 className="text-2xl font-bold text-secondary">
        Monitoring Dashboard
      </h1>
      <p className="mt-2 text-slate-600 dark:text-slate-400">
        Model performance metrics and drift detection status will be displayed
        here.
      </p>
    </Card>
  );
}
