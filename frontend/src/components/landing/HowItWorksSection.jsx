import React from 'react';
import { Play, UserPlus, LogIn, MessageSquare, CheckCircle2, ArrowRight, Zap, Sparkles, ChevronRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';

export default function HowItWorksSection() {
  const navigate = useNavigate();

  return (
    <section className="py-24 px-4 bg-[var(--background)] smooth-transition relative border-t border-[var(--card-border)] overflow-hidden">
      <div className="max-w-7xl mx-auto">
        
        <motion.div 
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.8 }}
          className="text-center mb-20"
        >
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-brand-500/10 border border-brand-500/20 text-brand-400 text-sm font-medium mb-6">
            <Play size={16} fill="currentColor" />
            Quick Start Guide
          </div>
          <h2 className="text-4xl md:text-5xl font-bold mb-6">How It Works</h2>
          <p className="text-xl text-foreground/70 max-w-2xl mx-auto">
            Get started in just 3 simple steps and unlock the power of AI-driven legal research.
          </p>
        </motion.div>

        <div className="space-y-12 md:space-y-24 relative before:absolute before:inset-0 before:ml-[3.5rem] md:before:mx-auto md:before:translate-x-0 before:w-0.5 before:bg-gradient-to-b before:from-brand-500/0 before:via-brand-500/20 before:to-brand-500/0">
          
          {/* Step 1 */}
          <motion.div 
            initial={{ opacity: 0, y: 50 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-100px" }}
            transition={{ duration: 0.8 }}
            className="relative flex flex-col md:flex-row items-center gap-8 md:gap-16"
          >
            <div className="flex-1 md:text-right flex flex-col items-start md:items-end w-full pl-24 md:pl-0">
              <span className="text-brand-500 font-bold tracking-widest text-sm mb-2 uppercase">Step 1</span>
              <h3 className="text-3xl font-bold mb-4">Create Your Account</h3>
              <p className="text-foreground/70 mb-6 text-lg">
                Sign up in seconds. No credit card required - start with our free tier 
                and explore all features immediately.
              </p>
              <ul className="space-y-3 mb-8 text-foreground/80">
                <li className="flex items-center gap-2 justify-start md:justify-end"><CheckCircle2 className="text-brand-500" size={18}/> Free registration</li>
                <li className="flex items-center gap-2 justify-start md:justify-end"><CheckCircle2 className="text-brand-500" size={18}/> Instant access</li>
              </ul>
              <button onClick={() => navigate('/auth?mode=register')} className="flex items-center gap-2 text-[var(--foreground)] bg-brand-600 hover:bg-brand-500 px-6 py-2.5 rounded-full font-medium transition-all hover:shadow-[0_0_20px_rgba(212,175,55,0.3)]">
                Sign Up Now <ArrowRight size={18} />
              </button>
            </div>
            
            <div className="absolute left-[3.5rem] md:left-1/2 -translate-x-1/2 w-16 h-16 rounded-full bg-[var(--card-bg)] smooth-transition border-4 border-neutral-800 flex items-center justify-center z-10 text-[var(--foreground)] group hover:border-brand-500 transition-colors duration-500">
              <UserPlus size={24} className="text-brand-400 group-hover:scale-110 transition-transform duration-300" />
            </div>

            <div className="flex-1 hidden md:block">
               <div className="w-full h-[300px] rounded-3xl bg-[var(--card-bg)] smooth-transition border border-[var(--card-border)] flex items-center justify-center hover:border-brand-500/30 transition-colors duration-500 group">
                  <UserPlus size={80} className="text-neutral-800 group-hover:text-neutral-700 transition-colors duration-500" />
               </div>
            </div>
          </motion.div>

          {/* Step 2 */}
          <motion.div 
            initial={{ opacity: 0, y: 50 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-100px" }}
            transition={{ duration: 0.8 }}
            className="relative flex flex-col md:flex-row-reverse items-center gap-8 md:gap-16"
          >
            <div className="flex-1 flex flex-col items-start w-full pl-24 md:pl-0">
              <span className="text-brand-400 font-bold tracking-widest text-sm mb-2 uppercase">Step 2</span>
              <h3 className="text-3xl font-bold mb-4">Navigate Your Dashboard</h3>
              <p className="text-foreground/70 mb-6 text-lg">
                 Once logged in, your dashboard provides instant access to all Civil Law features like Chatbot, Search, and judgments.
              </p>
              <ul className="space-y-3 mb-8 text-foreground/80">
                <li className="flex items-center gap-2"><CheckCircle2 className="text-brand-400" size={18}/> Chatbot Integration</li>
                <li className="flex items-center gap-2"><CheckCircle2 className="text-brand-400" size={18}/> Document Analyzer</li>
              </ul>
              <button onClick={() => navigate('/auth?mode=login')} className="flex items-center gap-2 text-[var(--foreground)] bg-foreground/5 hover:bg-[var(--background)] smooth-transitioneutral-700 px-6 py-2.5 rounded-full font-medium border border-[var(--card-border)] transition-all hover:border-brand-500/50">
                Login to Dashboard <ArrowRight size={18} />
              </button>
            </div>
            
            <div className="absolute left-[3.5rem] md:left-1/2 -translate-x-1/2 w-16 h-16 rounded-full bg-[var(--card-bg)] smooth-transition border-4 border-neutral-800 flex items-center justify-center z-10 text-[var(--foreground)] group hover:border-brand-400 transition-colors duration-500">
              <LogIn size={24} className="text-brand-400 group-hover:scale-110 transition-transform duration-300" />
            </div>

            <div className="flex-1 hidden md:block">
               <div className="w-full h-[300px] rounded-3xl bg-[var(--card-bg)] smooth-transition border border-[var(--card-border)] flex items-center justify-center hover:border-brand-500/30 transition-colors duration-500 group">
                  <LogIn size={80} className="text-neutral-800 group-hover:text-neutral-700 transition-colors duration-500" />
               </div>
            </div>
          </motion.div>

          {/* Step 3 */}
          <motion.div 
            initial={{ opacity: 0, y: 50 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-100px" }}
            transition={{ duration: 0.8 }}
            className="relative flex flex-col md:flex-row items-center gap-8 md:gap-16"
          >
            <div className="flex-1 md:text-right flex flex-col items-start md:items-end w-full pl-24 md:pl-0">
              <span className="text-brand-300 font-bold tracking-widest text-sm mb-2 uppercase">Step 3</span>
              <h3 className="text-3xl font-bold mb-4">Start Your Research</h3>
              <p className="text-foreground/70 mb-6 text-lg">
                Ask any question about contracts, property rights, or family matters. Our AI analyzes thousands of documents instantly to provide citation-backed answers.
              </p>
              <ul className="space-y-3 mb-8 text-foreground/80">
                <li className="flex items-center gap-2 justify-start md:justify-end"><CheckCircle2 className="text-brand-300" size={18}/> Instant Results</li>
                <li className="flex items-center gap-2 justify-start md:justify-end"><CheckCircle2 className="text-brand-300" size={18}/> Bilingual Support</li>
              </ul>
              <button onClick={() => navigate('/auth?mode=register')} className="flex items-center gap-2 text-black bg-white hover:bg-[var(--background)] smooth-transitioneutral-200 px-6 py-2.5 rounded-full font-bold transition-all hover:shadow-[0_0_30px_rgba(255,255,255,0.4)]">
                Get Started <Zap size={18} fill="currentColor" />
              </button>
            </div>
            
            <div className="absolute left-[3.5rem] md:left-1/2 -translate-x-1/2 w-16 h-16 rounded-full bg-[var(--card-bg)] smooth-transition border-4 border-neutral-800 flex items-center justify-center z-10 text-[var(--foreground)] group hover:border-brand-300 transition-colors duration-500">
              <MessageSquare size={24} className="text-brand-300 group-hover:scale-110 transition-transform duration-300" />
            </div>

            <div className="flex-1 hidden md:block">
               <div className="w-full h-[300px] rounded-3xl bg-[var(--card-bg)] smooth-transition border border-[var(--card-border)] flex items-center justify-center hover:border-brand-500/30 transition-colors duration-500 group">
                  <MessageSquare size={80} className="text-neutral-800 group-hover:text-neutral-700 transition-colors duration-500" />
               </div>
            </div>
          </motion.div>

        </div>

        {/* Call To Action Box */}
        <motion.div 
          initial={{ opacity: 0, scale: 0.95 }}
          whileInView={{ opacity: 1, scale: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.8 }}
          className="mt-32 w-full max-w-4xl mx-auto rounded-3xl bg-gradient-to-br from-brand-900/40 to-neutral-900/80 border border-brand-500/20 p-8 md:p-12 text-center relative overflow-hidden backdrop-blur-sm group hover:border-brand-500/50 transition-colors duration-500"
        >
           <div className="absolute inset-0 bg-[url('https://www.transparenttextures.com/patterns/cubes.png')] opacity-10 mix-blend-overlay"></div>
           
           {/* Moving gradient orb */}
           <div className="absolute -top-24 -right-24 w-48 h-48 bg-brand-500/30 blur-[50px] rounded-full group-hover:bg-brand-400/40 transition-colors duration-700"></div>
           <div className="absolute -bottom-24 -left-24 w-48 h-48 bg-brand-600/30 blur-[50px] rounded-full group-hover:bg-brand-500/40 transition-colors duration-700"></div>

           <div className="relative z-10">
             <Sparkles size={48} className="mx-auto mb-6 text-brand-300 animate-pulse" />
             <h3 className="text-3xl md:text-5xl font-bold mb-6 text-[var(--foreground)] leading-tight">Ready to Transform Your Legal Research?</h3>
             <p className="text-xl text-brand-100/70 mb-10 max-w-2xl mx-auto">Join hundreds of legal professionals already using Smart Lawyer in Pakistan.</p>
             <button 
               onClick={() => navigate('/auth?mode=register')}
               className="inline-flex items-center gap-2 bg-white text-black font-bold px-8 py-4 rounded-full hover:scale-105 hover:shadow-[0_0_40px_rgba(255,255,255,0.4)] transition-all duration-300"
             >
                Start Free Trial <ChevronRight size={20} />
             </button>
           </div>
        </motion.div>

      </div>
    </section>
  );
}






