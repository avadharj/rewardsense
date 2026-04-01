import Card from "../components/Card";

export default function ResultsPage() {
  return (
    <Card>
      <h1 className="text-2xl font-bold text-secondary">Your Results</h1>
      <p className="mt-2 text-slate-600 dark:text-slate-400">
        Your personalized credit card recommendations will appear here.
      </p>
    </Card>
  );
}
