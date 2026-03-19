import React, { Suspense } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'sonner';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider } from './context/AuthContext';
import { ThemeProvider } from './context/ThemeContext';
import ProtectedRoute from './routes/ProtectedRoute';
import ErrorBoundary from './components/ErrorBoundary';
import { ROUTES } from './constants/routes.constants';

// Initialize React Query Client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000,
      retry: 1,
    },
  },
});

// Layout
import DashboardLayout from './layouts/DashboardLayout';

// Lazy loading pages
const Landing = React.lazy(() => import('./pages/Landing'));
const Auth = React.lazy(() => import('./pages/Auth'));
const Dashboard = React.lazy(() => import('./pages/Dashboard'));
const Chatbot = React.lazy(() => import('./pages/Chatbot'));
const Contracts = React.lazy(() => import('./pages/Contracts'));
const Judgments = React.lazy(() => import('./pages/Judgments'));
const Search = React.lazy(() => import('./pages/Search'));

const GlobalSpinner = () => (
    <div className="min-h-screen flex items-center justify-center bg-[var(--background)]">
        <div className="w-10 h-10 border-4 border-[var(--card-border)] border-t-[var(--brand-500)] rounded-full animate-spin"></div>
    </div>
);

function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <AuthProvider>
            <Toaster position="top-right" richColors />
            <Router>
            <Suspense fallback={<GlobalSpinner />}>
              <Routes>
                {/* Public Routes */}
                <Route path={ROUTES.HOME} element={<Landing />} />
                <Route path={ROUTES.AUTH} element={<Auth />} />

                {/* Protected Routes inside the Layout */}
                <Route element={<ProtectedRoute />}>
                  <Route element={<DashboardLayout />}>
                    <Route path={ROUTES.DASHBOARD} element={<Dashboard />} />
                    <Route path="/chatbot" element={<Chatbot />} />
                    <Route path="/contracts" element={<Contracts />} />
                    <Route path="/judgments" element={<Judgments />} />
                    <Route path="/search" element={<Search />} />
                  </Route>
                </Route>
                
                {/* Catch-all fallback */}
                <Route path="*" element={<Navigate to={ROUTES.HOME} replace />} />
              </Routes>
            </Suspense>
          </Router>
        </AuthProvider>
      </ThemeProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  );
}

export default App;

