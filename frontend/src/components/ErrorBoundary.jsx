import React, { Component } from 'react';
import { IconAlertTriangle } from '@tabler/icons-react';

/**
 * Global Error Boundary
 * Catches JavaScript errors anywhere in their child component tree, logs those errors, 
 * and displays a fallback UI instead of the white screen of death.
 */
class ErrorBoundary extends Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, errorInfo: null };
    }

    static getDerivedStateFromError(error) {
        // Update state so the next render will show the fallback UI.
        return { hasError: true, errorInfo: error };
    }

    componentDidCatch(error, errorInfo) {
        // You can also log the error to an error reporting service like Sentry or LogRocket here
        console.error("ErrorBoundary caught an error:", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            // If explicitly given a fallback component to render
            if (this.props.fallback) {
                return this.props.fallback;
            }

            const isLocal = this.props.variant === 'local';

            return (
                <div className={`${isLocal ? 'h-full w-full rounded-2xl border border-red-500/20 bg-red-500/5' : 'min-h-screen'} flex flex-col items-center justify-center bg-[var(--background)] p-6 text-center`}>
                    <div className="w-16 h-16 bg-red-500/10 text-red-500 rounded-full flex items-center justify-center mb-4">
                        <IconAlertTriangle size={32} />
                    </div>
                    <h1 className={`${isLocal ? 'text-lg' : 'text-2xl'} font-bold text-[var(--foreground)] mb-2`}>
                        {isLocal ? 'Module Error' : 'Oops! Something went wrong.'}
                    </h1>
                    <p className="text-[var(--text-soft)] max-w-md mb-6 text-sm">
                        {isLocal 
                            ? "This specific component crashed. Exploring other areas of the app is safe." 
                            : "Our system encountered an unexpected error. Please try refreshing the page or navigating back."}
                    </p>
                    <button 
                        onClick={() => {
                            if(isLocal) {
                                this.setState({ hasError: false, errorInfo: null });
                            } else {
                                window.location.href = '/dashboard';
                            }
                        }}
                        className="px-6 py-2 bg-[var(--brand-500)] text-[#051326] font-bold rounded-full hover:opacity-90 transition-colors shadow-lg"
                    >
                        {isLocal ? 'Try Rendering Again' : 'Back to Dashboard'}
                    </button>
                </div>
            );
        }

        return this.props.children; 
    }
}

export default ErrorBoundary;
