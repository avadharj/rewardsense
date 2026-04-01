import { useState, useEffect } from "react";
import { useLocation, Navigate, Link } from "react-router-dom";
import Card from "../components/Card";
import Badge from "../components/Badge";
import Button from "../components/Button";
import type { PredictionResponse } from "../types";

export default function ResultsPage() {
  const location = useLocation();

  // Capture state once on mount — survives the history.state clearing below
  const [results] = useState<PredictionResponse | null>(
    () => location.state as PredictionResponse | null,
  );

  // Clear history.state so a page refresh will have null state and trigger redirect
  useEffect(() => {
    if (results) {
      window.history.replaceState({}, document.title);
    }
  }, [results]);

  if (!results) {
    return <Navigate to="/recommend" replace />;
  }

  return (
    <div className="max-w-3xl mx-auto">
      <div className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-secondary">Your Results</h1>
          <p className="mt-1 text-sm text-slate-500 dark:text-slate-400">
            Model {results.model_version} &middot;{" "}
            {results.inference_latency_ms}ms inference
          </p>
        </div>
        <Link to="/recommend">
          <Button variant="secondary" size="sm">
            Try Different Profile
          </Button>
        </Link>
      </div>

      <div className="space-y-4">
        {results.recommended_cards.map((card) => (
          <Card key={card.rank}>
            <div className="flex items-start justify-between mb-3">
              <div>
                <div className="flex items-center gap-2">
                  <h2 className="text-lg font-bold text-secondary">
                    {card.card_name}
                  </h2>
                  {card.rank === 1 && (
                    <Badge variant="success">Top Pick</Badge>
                  )}
                </div>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  {card.issuer} &middot; ${card.annual_fee}/yr &middot;{" "}
                  {card.reward_rate}% avg rewards
                </p>
              </div>
              <div className="text-right">
                <div className="text-2xl font-bold text-primary">
                  {card.score.toFixed(1)}
                </div>
                <div className="text-xs text-slate-500 dark:text-slate-400">
                  match score
                </div>
              </div>
            </div>

            {/* Score breakdown bar */}
            <div className="mb-3">
              <div className="flex items-center justify-between text-xs text-slate-500 dark:text-slate-400 mb-1">
                <span>
                  Deterministic: {card.score_breakdown.deterministic.toFixed(1)}
                </span>
                <span>
                  Personalization:{" "}
                  {card.score_breakdown.personalization.toFixed(1)}
                </span>
              </div>
              <div className="h-2 rounded-full bg-border overflow-hidden flex">
                <div
                  className="bg-primary/70 h-full"
                  style={{
                    width: `${(card.score_breakdown.deterministic / (card.score_breakdown.deterministic + card.score_breakdown.personalization)) * 100}%`,
                  }}
                />
                <div
                  className="bg-accent/70 h-full"
                  style={{
                    width: `${(card.score_breakdown.personalization / (card.score_breakdown.deterministic + card.score_breakdown.personalization)) * 100}%`,
                  }}
                />
              </div>
              <div className="flex items-center gap-4 mt-1 text-xs text-slate-500 dark:text-slate-400">
                <span className="flex items-center gap-1">
                  <span className="inline-block w-2 h-2 rounded-full bg-primary/70" />
                  Deterministic
                </span>
                <span className="flex items-center gap-1">
                  <span className="inline-block w-2 h-2 rounded-full bg-accent/70" />
                  Personalization
                </span>
              </div>
            </div>

            {/* Benefits */}
            <div className="flex flex-wrap gap-1.5 mb-3">
              {card.key_benefits.map((b) => (
                <Badge key={b} variant="info">
                  {b}
                </Badge>
              ))}
            </div>

            {/* Explanation */}
            <p className="text-sm text-slate-600 dark:text-slate-400 leading-relaxed">
              {card.explanation}
            </p>
          </Card>
        ))}
      </div>
    </div>
  );
}
