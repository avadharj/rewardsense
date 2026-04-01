import Card from "../components/Card";

export default function RecommendPage() {
  return (
    <Card>
      <h1 className="text-2xl font-bold text-secondary">
        Get Recommendations
      </h1>
      <p className="mt-2 text-slate-600 dark:text-slate-400">
        Enter your spending profile to receive personalized credit card
        recommendations.
      </p>
    </Card>
  );
}
