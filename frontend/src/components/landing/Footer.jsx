import React from 'react';
import { Scale } from 'lucide-react';

export default function Footer() {
  return (
    <footer className="bg-[var(--background)] smooth-transition text-[var(--foreground)] border-t border-[var(--card-border)] pt-16 pb-8 px-4">
      <div className="max-w-7xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-12 mb-16">
          
          {/* Logo & Description */}
          <div className="flex flex-col gap-4">
            <div className="flex items-center gap-2 text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-brand-400 to-brand-300">
              <Scale size={28} className="text-brand-500" />
              <span>Smart Lawyer</span>
            </div>
            <p className="text-foreground/60 leading-relaxed">
              AI-powered legal research platform for Pakistani civil law. 
              Making legal knowledge accessible to everyone.
            </p>
          </div>

          {/* Quick Links */}
          <div>
            <h4 className="text-lg font-semibold mb-4 text-[var(--foreground)]">Quick Links</h4>
            <ul className="space-y-3 text-foreground/70">
              <li><a href="#home" className="hover:text-brand-400 transition-colors">Home</a></li>
              <li><a href="#about" className="hover:text-brand-400 transition-colors">About</a></li>
              <li><a href="#features" className="hover:text-brand-400 transition-colors">Features</a></li>
              <li><a href="/auth?mode=login" className="hover:text-brand-400 transition-colors">Login</a></li>
            </ul>
          </div>

          {/* Features */}
          <div>
            <h4 className="text-lg font-semibold mb-4 text-[var(--foreground)]">Features</h4>
            <ul className="space-y-3 text-foreground/70">
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Legal Search</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Judgment Analysis</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Contract Summarization</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Bilingual Support</span></li>
            </ul>
          </div>

          {/* Legal */}
          <div>
            <h4 className="text-lg font-semibold mb-4 text-[var(--foreground)]">Legal</h4>
            <ul className="space-y-3 text-foreground/70">
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Privacy Policy</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Terms of Service</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Disclaimer</span></li>
              <li><span className="cursor-pointer hover:text-brand-400 transition-colors">Contact Us</span></li>
            </ul>
          </div>

        </div>

        <div className="border-t border-[var(--card-border)] pt-8 flex flex-col md:flex-row justify-between items-center gap-4 text-foreground/60 text-sm">
          <p>&copy; {new Date().getFullYear()} Smart Lawyer. All rights reserved.</p>
          <p className="max-w-xl text-center md:text-right">
            This platform provides AI-generated legal information for research purposes only. 
            It is not a substitute for professional legal advice.
          </p>
        </div>
      </div>
    </footer>
  );
}






