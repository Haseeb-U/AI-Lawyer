import React from 'react';
import { FloatingNav } from '../components/ui/floating-navbar';
import HeroSection from '../components/landing/HeroSection';
import AboutSection from '../components/landing/AboutSection';
import FeaturesSection from '../components/landing/FeaturesSection';
import HowItWorksSection from '../components/landing/HowItWorksSection';
import CivilLawCoverage from '../components/landing/CivilLawCoverage';
import Footer from '../components/landing/Footer';
import { Scale, Info, Play, Layers } from 'lucide-react';

export default function Landing() {
  const navItems = [
    { name: 'Home', link: '#home', icon: <Scale className="h-4 w-4 text-brand-500" /> },
    { name: 'About', link: '#about', icon: <Info className="h-4 w-4 text-brand-500" /> },
    { name: 'Features', link: '#features', icon: <Layers className="h-4 w-4 text-brand-500" /> },
    { name: 'How it Works', link: '#how-it-works', icon: <Play className="h-4 w-4 text-brand-500" /> },
  ];

  return (
    <div className="bg-[var(--background)] smooth-transition font-sans text-[var(--foreground)] min-h-screen selection:bg-brand-500/30 w-full overflow-x-hidden">
      <FloatingNav navItems={navItems} />
      <main className="relative z-10">
        <section id="home">
          <HeroSection />
        </section>
        <section id="about" className="bg-[var(--card-bg)] smooth-transition border-t border-[var(--card-border)]">
          <AboutSection />
        </section>
        <section id="civil-law" className="bg-[var(--background)] smooth-transition">
          <CivilLawCoverage />
        </section>
        <section id="features" className="bg-[var(--card-bg)] smooth-transition border-t border-[var(--card-border)] relative overflow-hidden">
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full max-w-4xl aspect-square bg-brand-500/10 rounded-full blur-3xl pointer-events-none"></div>
          <FeaturesSection />
        </section>
        <section id="how-it-works" className="bg-[var(--background)] smooth-transition relative">
          <HowItWorksSection />
        </section>
      </main>
      <Footer />
    </div>
  );
}




