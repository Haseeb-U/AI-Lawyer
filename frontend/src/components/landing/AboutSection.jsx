import React from 'react';
import { CheckCircle2 } from 'lucide-react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';

export default function AboutSection() {
  const aboutFeatures = [
    {
      title: "RAG-Powered Accuracy",
      desc: "Retrieval-Augmented Generation ensures precise, citation-backed responses."
    },
    {
      title: "Comprehensive Database",
      desc: "Access to Supreme Court judgments, High Court decisions, and civil law statutes."
    },
    {
      title: "User-Friendly Interface",
      desc: "Intuitive design that makes legal research simple and efficient for everyone."
    }
  ];

  return (
    <section id="about" className="py-24 px-4 bg-[var(--background)] smooth-transition relative">
      <div className="max-w-7xl mx-auto grid lg:grid-cols-2 gap-16 items-center">
        
        {/* Left text content */}
        <motion.div 
          initial={{ opacity: 0, x: -40 }}
          whileInView={{ opacity: 1, x: 0 }}
          viewport={{ once: true, margin: "-100px" }}
          transition={{ duration: 0.8, ease: "easeOut" }}
          className="flex flex-col gap-6"
        >
          <div className="inline-block text-brand-400 font-semibold tracking-wider uppercase text-sm mb-2">
            Your Legal Companion
          </div>
          <h2 className="text-4xl md:text-5xl font-bold leading-tight">
            About <span className="text-transparent bg-clip-text bg-gradient-to-r from-brand-400 to-brand-300">Smart Lawyer</span>
          </h2>
          
          <div className="space-y-6 text-foreground/70 text-lg leading-relaxed">
            <p>
              <strong className="text-foreground/90 font-semibold">Smart Lawyer</strong> is Pakistan's first AI-powered legal assistant exclusively designed 
              for Civil Law matters. Our platform specializes in Contract Law, Property Law, Family Law, Tort Law, 
              and Civil Procedure, combining cutting-edge artificial intelligence with Pakistan's comprehensive 
              civil law framework.
            </p>
            <p>
              Whether you're a legal professional researching case precedents, a law student studying civil statutes, 
              or a citizen seeking guidance on property rights, inheritance, or contract disputes, Smart Lawyer 
              provides instant, accurate, and citation-backed legal information. Our bilingual interface (English & Urdu) 
              ensures accessibility for all Pakistanis.
            </p>
          </div>

          <div className="mt-8 space-y-6">
            {aboutFeatures.map((item, idx) => (
              <motion.div 
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: idx * 0.2 }}
                key={idx} 
                className="flex gap-4 group"
              >
                <div className="mt-1">
                  <CheckCircle2 className="text-brand-500 group-hover:scale-110 group-hover:text-brand-400 transition-all duration-300" size={24} />
                </div>
                <div>
                  <h4 className="text-xl font-semibold text-foreground/90 mb-1 group-hover:text-brand-300 transition-colors">{item.title}</h4>
                  <p className="text-foreground/60">{item.desc}</p>
                </div>
              </motion.div>
            ))}
          </div>
        </motion.div>

        {/* Right graphical representation */}
        <motion.div 
          initial={{ opacity: 0, x: 40 }}
          whileInView={{ opacity: 1, x: 0 }}
          viewport={{ once: true, margin: "-100px" }}
          transition={{ duration: 0.8, ease: "easeOut" }}
          className="relative w-full aspect-square md:aspect-auto md:h-[600px] flex items-center justify-center group"
        >
          <div className="absolute inset-0 bg-brand-500/20 blur-[100px] rounded-full group-hover:bg-brand-500/30 transition-all duration-700" />
          <div className="relative w-full h-[400px] md:h-full bg-[var(--card-bg)] smooth-transition rounded-3xl border border-[var(--card-border)] overflow-hidden flex items-center justify-center p-8 group-hover:border-brand-500/30 transition-colors duration-500 shadow-2xl">
            <img 
              src="/aboutAI.png" 
              alt="AI Legal Intelligence" 
              className="object-cover w-full h-full rounded-2xl opacity-80 mix-blend-luminosity hover:mix-blend-normal hover:scale-105 transition-all duration-700" 
              onError={(e) => {
                 e.target.style.display = 'none';
                 e.target.parentElement.innerHTML = `<div class="text-center text-foreground/60"><svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="mx-auto mb-4 opacity-50"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><line x1="8" y1="16" x2="8" y2="16"/><line x1="16" y1="16" x2="16" y2="16"/></svg><p>AI Concept Graphic</p></div>`;
              }}
            />
          </div>
        </motion.div>

      </div>
    </section>
  );
}






