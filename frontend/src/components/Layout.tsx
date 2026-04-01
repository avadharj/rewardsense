import { Link, NavLink, Outlet } from "react-router-dom";
import ThemeToggle from "./ThemeToggle";

const navLinks = [
  { to: "/", label: "Home" },
  { to: "/recommend", label: "Recommend" },
  { to: "/dashboard", label: "Dashboard" },
];

export default function Layout() {
  return (
    <div className="min-h-screen flex flex-col bg-surface font-sans transition-colors duration-200">
      <header className="sticky top-0 z-50 bg-card border-b border-border transition-colors duration-200">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <Link to="/" className="text-xl font-bold text-primary">
            RewardSense
          </Link>
          <div className="flex items-center gap-1">
            <ThemeToggle />
            <nav className="flex items-center gap-1">
              {navLinks.map((link) => (
                <NavLink
                  key={link.to}
                  to={link.to}
                  end={link.to === "/"}
                  className={({ isActive }) =>
                    `px-3 py-2 rounded-md text-sm font-medium transition-colors duration-200 ${
                      isActive
                        ? "bg-primary-light text-primary dark:text-blue-300"
                        : "text-slate-600 hover:text-secondary hover:bg-slate-100 dark:text-slate-400 dark:hover:text-slate-200 dark:hover:bg-slate-700"
                    }`
                  }
                >
                  {link.label}
                </NavLink>
              ))}
            </nav>
          </div>
        </div>
      </header>

      <main className="flex-1">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <Outlet />
        </div>
      </main>

      <footer className="border-t border-border bg-card transition-colors duration-200">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-6 text-center text-sm text-slate-500 dark:text-slate-400">
          RewardSense &copy; 2026. Built with MLOps.
        </div>
      </footer>
    </div>
  );
}
